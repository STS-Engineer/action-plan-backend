import logging
import os
import re
import uuid
from pathlib import Path

from fastapi import HTTPException, UploadFile


logger = logging.getLogger(__name__)

DEFAULT_MAX_ATTACHMENT_SIZE_MB = 10
DEFAULT_ALLOWED_ATTACHMENT_EXTENSIONS = {
    "pdf",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "png",
    "jpg",
    "jpeg",
    "txt",
    "csv",
}
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RELATIVE_UPLOAD_ROOT = Path("uploads")
UPLOAD_ROOT = PROJECT_ROOT / RELATIVE_UPLOAD_ROOT


def get_upload_root() -> Path:
    return UPLOAD_ROOT.resolve()


def get_max_attachment_size_mb() -> float:
    raw_value = os.getenv("MAX_ATTACHMENT_SIZE_MB")

    if not raw_value:
        return float(DEFAULT_MAX_ATTACHMENT_SIZE_MB)

    try:
        value = float(raw_value)
    except ValueError:
        return float(DEFAULT_MAX_ATTACHMENT_SIZE_MB)

    if value <= 0:
        return float(DEFAULT_MAX_ATTACHMENT_SIZE_MB)

    return value


def get_max_attachment_size_bytes() -> int:
    return int(get_max_attachment_size_mb() * 1024 * 1024)


def get_allowed_attachment_extensions() -> set[str]:
    raw_value = os.getenv("ALLOWED_ATTACHMENT_EXTENSIONS")

    if not raw_value:
        return set(DEFAULT_ALLOWED_ATTACHMENT_EXTENSIONS)

    extensions = {
        extension.strip().lower().lstrip(".")
        for extension in raw_value.split(",")
        if extension.strip().lstrip(".")
    }

    return extensions or set(DEFAULT_ALLOWED_ATTACHMENT_EXTENSIONS)


def sanitize_original_filename(filename: str | None) -> str:
    basename = (filename or "attachment").replace("\\", "/").split("/")[-1].strip()
    basename = basename.replace("\x00", "")

    if not basename or basename in {".", ".."}:
        basename = "attachment"

    suffix = Path(basename).suffix
    stem = basename[: -len(suffix)] if suffix else basename

    safe_stem = re.sub(r"[^A-Za-z0-9._ -]", "_", stem)
    safe_stem = re.sub(r"\s+", " ", safe_stem).strip(" ._-")

    safe_suffix = re.sub(r"[^A-Za-z0-9.]", "", suffix)

    if not safe_stem:
        safe_stem = "attachment"

    return f"{safe_stem}{safe_suffix}"


def get_file_extension(filename: str | None) -> str:
    return Path(sanitize_original_filename(filename)).suffix.lower().lstrip(".")


def format_max_attachment_size_mb() -> str:
    max_size_mb = get_max_attachment_size_mb()

    if max_size_mb.is_integer():
        return str(int(max_size_mb))

    return f"{max_size_mb:g}"


def validate_attachment_file(file: UploadFile):
    original_filename = sanitize_original_filename(file.filename)
    extension = get_file_extension(original_filename)
    allowed_extensions = get_allowed_attachment_extensions()

    if not extension or extension not in allowed_extensions:
        logger.warning("Rejected attachment extension: %s", extension or "<none>")
        raise HTTPException(status_code=400, detail="File type not allowed.")

    max_size_bytes = get_max_attachment_size_bytes()
    current_position = file.file.tell()

    file.file.seek(0, os.SEEK_END)
    file_size = file.file.tell()
    file.file.seek(0)

    if file_size > max_size_bytes:
        logger.warning("Rejected attachment size: %s bytes", file_size)
        raise HTTPException(
            status_code=413,
            detail=(
                "Attachment too large. "
                f"Maximum size is {format_max_attachment_size_mb()} MB."
            ),
        )

    if current_position:
        file.file.seek(0)

    return {
        "file_name": original_filename,
        "extension": extension,
        "size": file_size,
    }


def build_safe_stored_filename(original_filename: str) -> str:
    extension = get_file_extension(original_filename)
    suffix = f".{extension}" if extension else ""

    return f"{uuid.uuid4().hex}{suffix}"


def assert_path_under_upload_root(path) -> Path:
    upload_root = get_upload_root()
    raw_path = str(path).replace("\\", os.sep)
    candidate_path = Path(raw_path)

    if not candidate_path.is_absolute():
        candidate_path = PROJECT_ROOT / candidate_path

    resolved_path = candidate_path.resolve()

    try:
        is_inside_upload_root = (
            os.path.commonpath([str(upload_root), str(resolved_path)]) == str(upload_root)
        )
    except ValueError:
        is_inside_upload_root = False

    if not is_inside_upload_root:
        raise HTTPException(status_code=404, detail="Attachment file not found")

    return resolved_path
