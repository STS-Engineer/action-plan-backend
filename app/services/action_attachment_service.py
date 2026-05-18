import os
import mimetypes
import logging
from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.action_attachment import ActionAttachment
from app.services.azure_blob_service import (
    delete_blob_if_exists,
    generate_blob_download_url,
    upload_action_attachment_blob,
)
from app.services.action_access_service import can_access_action
from app.services.action_attachment_security_service import (
    assert_path_under_upload_root,
    sanitize_original_filename,
    validate_attachment_file,
)


logger = logging.getLogger(__name__)


def is_legacy_local_attachment_path(file_path: str | None) -> bool:
    normalized_path = str(file_path or "").replace("\\", "/").lower()

    return normalized_path.startswith("uploads/")


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
):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return {"error": "Action not found"}

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

    relative_file_path = upload_action_attachment_blob(
        action_id=action_id,
        file_name=original_filename,
        file_bytes=file_bytes,
        content_type=file.content_type,
    )

    try:
        attachment = ActionAttachment(
            action_id=action_id,
            file_name=original_filename,
            file_path=relative_file_path,
            uploaded_by=uploaded_by,
        )
        db.add(attachment)
        db.commit()
    except Exception as exc:
        db.rollback()
        try:
            delete_blob_if_exists(relative_file_path)
        except HTTPException:
            logger.exception("Failed to delete orphan attachment blob: %s", relative_file_path)
        raise HTTPException(
            status_code=500,
            detail="Failed to save attachment metadata.",
        ) from exc

    db.refresh(attachment)

    return attachment_to_dict(attachment)


async def get_action_attachments_service(action_id: int, db: Session):
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
):
    attachment = (
        db.query(ActionAttachment)
        .filter(ActionAttachment.id == attachment_id)
        .first()
    )

    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found")

    action = db.query(Action).filter(Action.id == attachment.action_id).first()

    if not action:
        raise HTTPException(status_code=404, detail="Attachment not found")

    access = can_access_action(logged_user_email, action, directory_db)

    if not access["allowed"]:
        raise HTTPException(status_code=403, detail="Forbidden")

    if not is_legacy_local_attachment_path(attachment.file_path):
        return {
            "download_url": generate_blob_download_url(attachment.file_path),
            "file_name": attachment.file_name,
        }

    try:
        file_path = assert_path_under_upload_root(attachment.file_path)
    except HTTPException as exc:
        raise HTTPException(
            status_code=404,
            detail="Legacy local attachment not available",
        ) from exc

    if not os.path.isfile(file_path):
        raise HTTPException(
            status_code=404,
            detail="Legacy local attachment not available",
        )

    media_type = mimetypes.guess_type(attachment.file_name)[0] or "application/octet-stream"

    return FileResponse(
        path=file_path,
        filename=attachment.file_name,
        media_type=media_type,
    )
