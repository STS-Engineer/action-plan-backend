import os
import shutil
from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.action_attachment import ActionAttachment


UPLOAD_ROOT = "uploads"


async def upload_action_attachment_service(
    action_id: int,
    file: UploadFile,
    db: Session,
    uploaded_by: str | None = None,
):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return {"error": "Action not found"}

    action_folder = os.path.join(UPLOAD_ROOT, f"action_{action_id}")
    os.makedirs(action_folder, exist_ok=True)

    safe_filename = file.filename.replace("/", "_").replace("\\", "_")
    file_path = os.path.join(action_folder, safe_filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    attachment = ActionAttachment(
        action_id=action_id,
        file_name=safe_filename,
        file_path=file_path,
        uploaded_by=uploaded_by,
    )

    db.add(attachment)
    db.commit()
    db.refresh(attachment)

    return {
        "id": attachment.id,
        "action_id": attachment.action_id,
        "file_name": attachment.file_name,
        "file_path": attachment.file_path,
        "uploaded_by": attachment.uploaded_by,
        "created_at": attachment.created_at,
    }


async def get_action_attachments_service(action_id: int, db: Session):
    attachments = (
        db.query(ActionAttachment)
        .filter(ActionAttachment.action_id == action_id)
        .order_by(ActionAttachment.created_at.desc())
        .all()
    )

    return [
        {
            "id": attachment.id,
            "action_id": attachment.action_id,
            "file_name": attachment.file_name,
            "file_path": attachment.file_path,
            "uploaded_by": attachment.uploaded_by,
            "created_at": attachment.created_at,
        }
        for attachment in attachments
    ]


async def download_action_attachment_service(attachment_id: int, db: Session):
    attachment = (
        db.query(ActionAttachment)
        .filter(ActionAttachment.id == attachment_id)
        .first()
    )

    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found")

    upload_root = os.path.abspath(UPLOAD_ROOT)
    file_path = os.path.abspath(attachment.file_path)

    try:
        is_inside_upload_root = os.path.commonpath([upload_root, file_path]) == upload_root
    except ValueError:
        is_inside_upload_root = False

    if not is_inside_upload_root:
        raise HTTPException(status_code=404, detail="Attachment file not found")

    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Attachment file not found")

    return FileResponse(
        path=file_path,
        filename=attachment.file_name,
        media_type="application/octet-stream",
    )
