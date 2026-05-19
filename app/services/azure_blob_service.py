import logging
import mimetypes
import os
import uuid
from datetime import datetime, timedelta, timezone

from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    BlobSasPermissions,
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
)
from dotenv import load_dotenv
from fastapi import HTTPException


load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_CONTAINER_NAME = "action-plan-files"
SAS_EXPIRATION_MINUTES = 10
ATTACHMENT_STORAGE_UNAVAILABLE_MESSAGE = "Attachment storage temporarily unavailable."
ATTACHMENT_FILE_NOT_FOUND_MESSAGE = "Attachment file not found."
_container_clients = {}


class AttachmentStorageError(Exception):
    pass


class AttachmentStorageConfigError(AttachmentStorageError):
    pass


class AttachmentStorageUnavailableError(AttachmentStorageError):
    pass


def _raise_storage_unavailable(exc: Exception):
    raise HTTPException(
        status_code=503,
        detail=ATTACHMENT_STORAGE_UNAVAILABLE_MESSAGE,
    ) from exc


def get_azure_blob_diagnostics() -> dict:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    container_name = get_azure_storage_container_name()

    return {
        "enabled": bool(connection_string and container_name),
        "container_name": container_name,
    }


def log_azure_blob_configuration() -> None:
    diagnostics = get_azure_blob_diagnostics()

    logger.info(
        "Azure Blob attachment storage: enabled=%s container=%s",
        diagnostics["enabled"],
        diagnostics["container_name"],
    )

    if not diagnostics["enabled"]:
        logger.warning(
            "Azure Blob attachment storage is disabled or incomplete. "
            "Check AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_CONTAINER."
        )


def get_azure_storage_connection_string() -> str:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()

    if not connection_string:
        raise AttachmentStorageConfigError("AZURE_STORAGE_CONNECTION_STRING is not set.")

    return connection_string


def get_azure_storage_container_name() -> str:
    container_name = os.getenv("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER_NAME).strip()

    return container_name or DEFAULT_CONTAINER_NAME


def _parse_connection_string(connection_string: str) -> dict[str, str]:
    values = {}

    for part in connection_string.split(";"):
        if "=" not in part:
            continue

        key, value = part.split("=", 1)
        values[key] = value

    return values


def get_container_client():
    connection_string = get_azure_storage_connection_string()
    container_name = get_azure_storage_container_name()
    cache_key = (hash(connection_string), container_name)

    if cache_key in _container_clients:
        return _container_clients[cache_key]

    try:
        service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = service_client.get_container_client(container_name)

        try:
            container_client.create_container(public_access=None)
        except ResourceExistsError:
            pass

        _container_clients[cache_key] = container_client
        return container_client
    except AttachmentStorageError:
        raise
    except (AzureError, ValueError) as exc:
        logger.exception("Failed to initialize Azure Blob container.")
        raise AttachmentStorageUnavailableError("Failed to initialize Azure Blob container.") from exc


def build_action_attachment_blob_name(action_id: int, extension: str) -> str:
    safe_extension = extension.lower().lstrip(".")
    suffix = f".{safe_extension}" if safe_extension else ""

    return f"action_{action_id}/{uuid.uuid4().hex}{suffix}"


def upload_action_attachment_blob(
    action_id: int,
    file_name: str,
    file_bytes: bytes,
    content_type: str | None = None,
) -> str:
    extension = os.path.splitext(file_name)[1].lower().lstrip(".")
    blob_name = build_action_attachment_blob_name(action_id, extension)
    detected_content_type = (
        content_type
        or mimetypes.guess_type(file_name)[0]
        or "application/octet-stream"
    )

    try:
        container_name = get_azure_storage_container_name()
        logger.debug(
            "Attachment blob upload started action_id=%s blob_name=%s container=%s",
            action_id,
            blob_name,
            container_name,
        )
        container_client = get_container_client()
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            file_bytes,
            overwrite=False,
            content_settings=ContentSettings(content_type=detected_content_type),
        )
        logger.debug(
            "Attachment blob upload succeeded action_id=%s blob_name=%s container=%s",
            action_id,
            blob_name,
            container_name,
        )
        return blob_name
    except AttachmentStorageError as exc:
        logger.warning(
            "Attachment blob upload unavailable action_id=%s blob_name=%s container=%s reason=%s",
            action_id,
            blob_name,
            get_azure_storage_container_name(),
            exc,
        )
        _raise_storage_unavailable(exc)
    except AzureError as exc:
        logger.exception(
            "Attachment blob upload failed action_id=%s blob_name=%s container=%s",
            action_id,
            blob_name,
            get_azure_storage_container_name(),
        )
        _raise_storage_unavailable(exc)


def blob_exists(blob_name: str) -> bool:
    try:
        container_client = get_container_client()
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False
    except AttachmentStorageError as exc:
        logger.warning(
            "Attachment blob existence check unavailable blob_name=%s container=%s reason=%s",
            blob_name,
            get_azure_storage_container_name(),
            exc,
        )
        _raise_storage_unavailable(exc)
    except AzureError as exc:
        logger.exception(
            "Attachment blob existence check failed blob_name=%s container=%s",
            blob_name,
            get_azure_storage_container_name(),
        )
        _raise_storage_unavailable(exc)


def generate_blob_download_url(blob_name: str, verify_exists: bool = True) -> str:
    try:
        connection_string = get_azure_storage_connection_string()
        connection_values = _parse_connection_string(connection_string)
        account_name = connection_values.get("AccountName")
        account_key = connection_values.get("AccountKey")

        if not account_name or not account_key:
            raise AttachmentStorageConfigError(
                "AZURE_STORAGE_CONNECTION_STRING must include AccountName and AccountKey."
            )

        container_name = get_azure_storage_container_name()
        container_client = get_container_client()
        blob_client = container_client.get_blob_client(blob_name)

        if verify_exists:
            blob_client.get_blob_properties()

        expires_at = datetime.now(timezone.utc) + timedelta(minutes=SAS_EXPIRATION_MINUTES)
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expires_at,
        )

        return f"{blob_client.url}?{sas_token}"
    except ResourceNotFoundError as exc:
        raise HTTPException(status_code=404, detail=ATTACHMENT_FILE_NOT_FOUND_MESSAGE) from exc
    except AttachmentStorageError as exc:
        logger.warning(
            "Attachment download URL unavailable blob_name=%s container=%s reason=%s",
            blob_name,
            get_azure_storage_container_name(),
            exc,
        )
        _raise_storage_unavailable(exc)
    except (AzureError, ValueError) as exc:
        logger.exception(
            "Attachment download URL generation failed blob_name=%s container=%s",
            blob_name,
            get_azure_storage_container_name(),
        )
        _raise_storage_unavailable(exc)


def delete_blob_if_exists(blob_name: str) -> None:
    try:
        container_client = get_container_client()
        container_client.delete_blob(blob_name)
    except ResourceNotFoundError:
        return
    except AttachmentStorageError:
        raise
    except AzureError:
        logger.exception("Failed to delete orphan attachment blob: %s", blob_name)
