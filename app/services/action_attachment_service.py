import os
import mimetypes
import logging
from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.action_attachment import ActionAttachment
from app.services.azure_blob_service import (
    ATTACHMENT_FILE_NOT_FOUND_MESSAGE,
    delete_blob_if_exists,
    blob_exists,
    generate_blob_download_url,
    get_azure_storage_container_name,
    upload_action_attachment_blob,
)
from app.services.action_access_service import can_access_action
from app.services.action_status_logic_service import get_action_active_predicate
from app.services.action_attachment_security_service import (
    assert_path_under_upload_root,
    sanitize_original_filename,
    validate_attachment_file,
)


logger = logging.getLogger(__name__)


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


async def get_action_attachments_service(action_id: int, db: Session):
    action = (
        db.query(Action.id)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

    if not action:
        return []

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

        return {
            "download_url": generate_blob_download_url(attachment.file_path, verify_exists=False),
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

    return FileResponse(
        path=file_path,
        filename=attachment.file_name,
        media_type=media_type,
    )
