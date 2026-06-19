import os
import mimetypes
import logging
from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse
from azure.core.exceptions import AzureError, ResourceNotFoundError
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.action_attachment import ActionAttachment
from app.services.azure_blob_service import (
    ATTACHMENT_FILE_NOT_FOUND_MESSAGE,
    ATTACHMENT_STORAGE_UNAVAILABLE_MESSAGE,
    delete_blob_if_exists,
    blob_exists,
    generate_blob_download_url,
    get_azure_blob_diagnostics,
    get_azure_storage_container_name,
    get_container_client,
    upload_action_attachment_blob,
)
from app.services.action_access_service import can_access_action
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import get_action_active_predicate
from app.services.auth_service import is_admin
from app.services.action_attachment_security_service import (
    assert_path_under_upload_root,
    get_upload_root,
    sanitize_original_filename,
    validate_attachment_file,
)


logger = logging.getLogger(__name__)

UPLOAD_ENDPOINT = "/api/action_plan_action/actions/{action_id}/attachments"
DOWNLOAD_ENDPOINT = "/api/action_plan_action/attachments/{attachment_id}/download"


def is_legacy_local_attachment_path(file_path: str | None) -> bool:
    normalized_path = str(file_path or "").replace("\\", "/").lower()

    if normalized_path.startswith("uploads/"):
        return True

    try:
        assert_path_under_upload_root(file_path)
        return True
    except HTTPException:
        return False


def attachment_to_dict(attachment: ActionAttachment):
    return {
        "id": attachment.id,
        "action_id": attachment.action_id,
        "file_name": attachment.file_name,
        "file_path": attachment.file_path,
        "uploaded_by": attachment.uploaded_by,
        "created_at": attachment.created_at,
    }


def log_admin_attachment_download(
    db: Session,
    action_id: int,
    attachment_id: int,
    logged_user_email: str | None,
    current_user=None,
):
    if current_user is None or not is_admin(current_user):
        return

    log_action_event(
        db=db,
        action_id=action_id,
        event_type="admin_attachment_downloaded",
        old_value=None,
        new_value=str(attachment_id),
        details=f"Admin downloaded attachment {attachment_id}.",
        created_by=logged_user_email,
    )
    db.commit()


def _safe_storage_error(exc: Exception) -> str:
    detail = getattr(exc, "detail", None)

    if detail:
        return str(detail)

    return str(exc) or exc.__class__.__name__


def _get_storage_type() -> str:
    diagnostics = get_azure_blob_diagnostics()

    if diagnostics["enabled"]:
        return "azure_blob"

    return "azure_blob_unconfigured_with_local_legacy_fallback"


def _inspect_attachment_storage(attachment: ActionAttachment) -> dict:
    result = {
        "storage_type": "local" if is_legacy_local_attachment_path(attachment.file_path) else "azure_blob",
        "file_exists": False,
        "size": None,
        "error": None,
    }

    if result["storage_type"] == "local":
        try:
            file_path = assert_path_under_upload_root(attachment.file_path)
        except HTTPException as exc:
            result["error"] = _safe_storage_error(exc)
            return result

        result["file_exists"] = os.path.isfile(file_path)

        if result["file_exists"]:
            try:
                result["size"] = os.path.getsize(file_path)
            except OSError as exc:
                result["error"] = _safe_storage_error(exc)

        return result

    diagnostics = get_azure_blob_diagnostics()

    if not diagnostics["enabled"]:
        result["error"] = ATTACHMENT_STORAGE_UNAVAILABLE_MESSAGE
        return result

    try:
        blob_client = get_container_client().get_blob_client(attachment.file_path)
        properties = blob_client.get_blob_properties()
        result["file_exists"] = True
        result["size"] = getattr(properties, "size", None)
    except ResourceNotFoundError:
        result["file_exists"] = False
    except HTTPException as exc:
        result["error"] = _safe_storage_error(exc)
    except AzureError as exc:
        result["error"] = _safe_storage_error(exc)
    except Exception as exc:
        result["error"] = _safe_storage_error(exc)

    return result


def _count_orphan_local_files(db_attachments: list[ActionAttachment]) -> int:
    upload_root = get_upload_root()

    if not upload_root.exists():
        return 0

    referenced_local_paths = set()

    for attachment in db_attachments:
        if not is_legacy_local_attachment_path(attachment.file_path):
            continue

        try:
            referenced_local_paths.add(str(assert_path_under_upload_root(attachment.file_path)))
        except HTTPException:
            continue

    orphan_count = 0

    for file_path in upload_root.rglob("*"):
        if not file_path.is_file():
            continue

        if str(file_path.resolve()) not in referenced_local_paths:
            orphan_count += 1

    return orphan_count


def _count_orphan_azure_blobs(db_attachments: list[ActionAttachment]) -> tuple[int, str | None]:
    diagnostics = get_azure_blob_diagnostics()

    if not diagnostics["enabled"]:
        return 0, ATTACHMENT_STORAGE_UNAVAILABLE_MESSAGE

    referenced_blob_names = {
        str(attachment.file_path)
        for attachment in db_attachments
        if not is_legacy_local_attachment_path(attachment.file_path)
    }

    try:
        container_client = get_container_client()
        orphan_count = 0

        for blob in container_client.list_blobs():
            if blob.name not in referenced_blob_names:
                orphan_count += 1

        return orphan_count, None
    except HTTPException as exc:
        return 0, _safe_storage_error(exc)
    except AzureError as exc:
        return 0, _safe_storage_error(exc)
    except Exception as exc:
        return 0, _safe_storage_error(exc)


def get_attachment_health_service(db: Session) -> dict:
    diagnostics = get_azure_blob_diagnostics()
    attachments = db.query(ActionAttachment).all()
    last_errors = []

    orphan_db_records = (
        db.query(func.count(ActionAttachment.id))
        .outerjoin(Action, ActionAttachment.action_id == Action.id)
        .filter(Action.id.is_(None))
        .scalar()
        or 0
    )

    missing_files = 0
    unchecked_files = 0

    for attachment in attachments:
        storage_check = _inspect_attachment_storage(attachment)

        if storage_check["error"]:
            unchecked_files += 1
            last_errors.append(storage_check["error"])
            continue

        if not storage_check["file_exists"]:
            missing_files += 1

    orphan_local_files = _count_orphan_local_files(attachments)
    orphan_azure_blobs, azure_orphan_error = _count_orphan_azure_blobs(attachments)

    if azure_orphan_error:
        last_errors.append(azure_orphan_error)

    unique_errors = []

    for error in last_errors:
        if error and error not in unique_errors:
            unique_errors.append(error)

    return {
        "storage_type": _get_storage_type(),
        "storage_configured": diagnostics["enabled"],
        "container_name": diagnostics["container_name"],
        "upload_endpoint": UPLOAD_ENDPOINT,
        "download_endpoint": DOWNLOAD_ENDPOINT,
        "attachment_count": len(attachments),
        "orphan_db_records": orphan_db_records,
        "orphan_storage_files": orphan_local_files + orphan_azure_blobs,
        "orphan_local_files": orphan_local_files,
        "orphan_azure_blobs": orphan_azure_blobs,
        "missing_files": missing_files,
        "unchecked_files": unchecked_files,
        "last_error": unique_errors[0] if unique_errors else None,
    }


def get_attachment_audit_service(
    db: Session,
    logged_user_email: str | None = None,
    directory_db=None,
    current_user=None,
) -> list[dict]:
    attachments = (
        db.query(ActionAttachment)
        .order_by(ActionAttachment.created_at.desc(), ActionAttachment.id.desc())
        .limit(100)
        .all()
    )
    action_ids = {attachment.action_id for attachment in attachments}
    actions = {}

    if action_ids:
        actions = {
            action.id: action
            for action in db.query(Action).filter(Action.id.in_(action_ids)).all()
        }

    rows = []

    for attachment in attachments:
        action = actions.get(attachment.action_id)
        storage_check = _inspect_attachment_storage(attachment)
        access = {"allowed": False, "reason": "action_missing"}

        if action:
            access = can_access_action(
                logged_user_email,
                action,
                directory_db,
                user_role=getattr(current_user, "role", None),
                created_by_email=attachment.uploaded_by,
            )

        rows.append(
            {
                "id": attachment.id,
                "action_id": attachment.action_id,
                "filename": attachment.file_name,
                "size": storage_check["size"],
                "created_at": attachment.created_at,
                "storage_path": attachment.file_path,
                "storage_type": storage_check["storage_type"],
                "file_exists": storage_check["file_exists"],
                "file_check_error": storage_check["error"],
                "downloadable": bool(
                    action
                    and access["allowed"]
                    and storage_check["file_exists"]
                    and not storage_check["error"]
                ),
                "access_check_passed": access["allowed"],
                "access_reason": access.get("reason"),
            }
        )

    return rows


async def upload_action_attachment_service(
    action_id: int,
    file: UploadFile,
    db: Session,
    uploaded_by: str | None = None,
    logged_user_email: str | None = None,
    directory_db=None,
    current_user=None,
):
    action = (
        db.query(Action)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    access = can_access_action(
        logged_user_email,
        action,
        directory_db,
        user_role=getattr(current_user, "role", None),
    )
    authorized = access["allowed"]

    logger.debug(
        "Attachment upload authorization authenticated_user=%s action_id=%s authorized=%s scope=%s",
        logged_user_email,
        action_id,
        authorized,
        access.get("scope"),
    )

    if not authorized:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to access this attachment.",
        )

    validation = validate_attachment_file(file)
    original_filename = sanitize_original_filename(validation["file_name"])

    try:
        file.file.seek(0)
        file_bytes = await file.read()
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to read attachment file.",
        ) from exc

    try:
        relative_file_path = upload_action_attachment_blob(
            action_id=action_id,
            file_name=original_filename,
            file_bytes=file_bytes,
            content_type=file.content_type,
        )
        logger.debug(
            "Attachment upload succeeded authenticated_user=%s action_id=%s blob_name=%s container=%s",
            logged_user_email,
            action_id,
            relative_file_path,
            get_azure_storage_container_name(),
        )
    except HTTPException as exc:
        logger.warning(
            "Attachment upload failed authenticated_user=%s action_id=%s container=%s status_code=%s detail=%s",
            logged_user_email,
            action_id,
            get_azure_storage_container_name(),
            exc.status_code,
            exc.detail,
        )
        raise

    try:
        attachment = ActionAttachment(
            action_id=action_id,
            file_name=original_filename,
            file_path=relative_file_path,
            uploaded_by=logged_user_email or uploaded_by,
        )
        db.add(attachment)
        db.commit()
    except Exception as exc:
        db.rollback()
        try:
            delete_blob_if_exists(relative_file_path)
        except Exception:
            logger.exception("Failed to delete orphan attachment blob: %s", relative_file_path)
        raise HTTPException(
            status_code=500,
            detail="Failed to save attachment metadata.",
        ) from exc

    db.refresh(attachment)

    return attachment_to_dict(attachment)


async def get_action_attachments_service(
    action_id: int,
    db: Session,
    logged_user_email: str | None = None,
    directory_db=None,
    current_user=None,
):
    action = (
        db.query(Action)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

    if not action:
        return []

    if current_user is not None:
        access = can_access_action(
            logged_user_email,
            action,
            directory_db,
            user_role=getattr(current_user, "role", None),
        )

        if not access["allowed"]:
            raise HTTPException(
                status_code=403,
                detail="You do not have permission to access this attachment.",
            )

    attachments = (
        db.query(ActionAttachment)
        .filter(ActionAttachment.action_id == action_id)
        .order_by(ActionAttachment.created_at.desc())
        .all()
    )

    return [
        attachment_to_dict(attachment)
        for attachment in attachments
    ]


async def download_action_attachment_service(
    attachment_id: int,
    db: Session,
    logged_user_email: str,
    directory_db,
    current_user=None,
):
    logger.debug(
        "Attachment download requested attachment_id=%s authenticated_user=%s",
        attachment_id,
        logged_user_email,
    )

    attachment = (
        db.query(ActionAttachment)
        .filter(ActionAttachment.id == attachment_id)
        .first()
    )

    if not attachment:
        raise HTTPException(status_code=404, detail=ATTACHMENT_FILE_NOT_FOUND_MESSAGE)

    action = (
        db.query(Action)
        .filter(Action.id == attachment.action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )
    resolved_action_id = action.id if action else None

    if not action:
        raise HTTPException(status_code=404, detail=ATTACHMENT_FILE_NOT_FOUND_MESSAGE)

    access = can_access_action(
        logged_user_email,
        action,
        directory_db,
        user_role=getattr(current_user, "role", None),
        created_by_email=attachment.uploaded_by,
    )
    authorized = access["allowed"]

    logger.debug(
        (
            "Attachment download authorization attachment_id=%s action_id=%s "
            "logged_user_email=%s action_email_responsable=%s allowed=%s reason=%s scope=%s"
        ),
        attachment_id,
        resolved_action_id,
        logged_user_email,
        action.email_responsable,
        authorized,
        access.get("reason"),
        access.get("scope"),
    )

    if not authorized:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to access this attachment.",
        )

    if not is_legacy_local_attachment_path(attachment.file_path):
        exists = blob_exists(attachment.file_path)

        logger.debug(
            "Attachment download blob check attachment_id=%s resolved_action_id=%s blob_name=%s container=%s blob_exists=%s",
            attachment_id,
            resolved_action_id,
            attachment.file_path,
            get_azure_storage_container_name(),
            exists,
        )

        if not exists:
            raise HTTPException(status_code=404, detail=ATTACHMENT_FILE_NOT_FOUND_MESSAGE)

        download_url = generate_blob_download_url(attachment.file_path, verify_exists=False)

        log_admin_attachment_download(
            db=db,
            action_id=action.id,
            attachment_id=attachment.id,
            logged_user_email=logged_user_email,
            current_user=current_user,
        )

        return {
            "download_url": download_url,
            "file_name": attachment.file_name,
        }

    try:
        file_path = assert_path_under_upload_root(attachment.file_path)
    except HTTPException as exc:
        raise HTTPException(
            status_code=404,
            detail=ATTACHMENT_FILE_NOT_FOUND_MESSAGE,
        ) from exc

    if not os.path.isfile(file_path):
        logger.debug(
            "Attachment download local file check attachment_id=%s resolved_action_id=%s path=%s blob_exists=false",
            attachment_id,
            resolved_action_id,
            file_path,
        )
        raise HTTPException(
            status_code=404,
            detail=ATTACHMENT_FILE_NOT_FOUND_MESSAGE,
        )

    logger.debug(
        "Attachment download local file check attachment_id=%s resolved_action_id=%s path=%s blob_exists=true",
        attachment_id,
        resolved_action_id,
        file_path,
    )

    media_type = mimetypes.guess_type(attachment.file_name)[0] or "application/octet-stream"

    log_admin_attachment_download(
        db=db,
        action_id=action.id,
        attachment_id=attachment.id,
        logged_user_email=logged_user_email,
        current_user=current_user,
    )

    return FileResponse(
        path=file_path,
        filename=attachment.file_name,
        media_type=media_type,
    )
