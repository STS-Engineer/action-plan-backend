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
from fastapi import HTTPException


logger = logging.getLogger(__name__)

DEFAULT_CONTAINER_NAME = "action-plan-files"
SAS_EXPIRATION_MINUTES = 10
_container_clients = {}


def get_azure_storage_connection_string() -> str:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()

    if not connection_string:
        raise HTTPException(
            status_code=500,
            detail="Azure storage is not configured.",
        )

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
    except HTTPException:
        raise
    except (AzureError, ValueError) as exc:
        logger.exception("Failed to initialize Azure Blob container.")
        raise HTTPException(
            status_code=500,
            detail="Failed to initialize attachment storage.",
        ) from exc


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
        container_client = get_container_client()
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            file_bytes,
            overwrite=False,
            content_settings=ContentSettings(content_type=detected_content_type),
        )
        logger.debug("Saved attachment blob: %s", blob_name)
        return blob_name
    except HTTPException:
        raise
    except AzureError as exc:
        logger.exception("Failed to upload attachment blob.")
        raise HTTPException(
            status_code=500,
            detail="Failed to upload attachment file.",
        ) from exc


def generate_blob_download_url(blob_name: str) -> str:
    connection_string = get_azure_storage_connection_string()
    connection_values = _parse_connection_string(connection_string)
    account_name = connection_values.get("AccountName")
    account_key = connection_values.get("AccountKey")

    if not account_name or not account_key:
        raise HTTPException(
            status_code=500,
            detail="Azure storage SAS is not configured.",
        )

    try:
        container_name = get_azure_storage_container_name()
        container_client = get_container_client()
        blob_client = container_client.get_blob_client(blob_name)
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
        raise HTTPException(status_code=404, detail="Attachment file not found") from exc
    except HTTPException:
        raise
    except (AzureError, ValueError) as exc:
        logger.exception("Failed to generate attachment download URL.")
        raise HTTPException(
            status_code=500,
            detail="Failed to generate attachment download URL.",
        ) from exc


def delete_blob_if_exists(blob_name: str) -> None:
    try:
        container_client = get_container_client()
        container_client.delete_blob(blob_name)
    except ResourceNotFoundError:
        return
    except HTTPException:
        raise
    except AzureError:
        logger.exception("Failed to delete orphan attachment blob: %s", blob_name)
