import os
from urllib.parse import urlencode


def build_action_frontend_url(action_id: int) -> str:
    base_url = (
        os.getenv("FRONTEND_BASE_URL")
        or os.getenv("FRONTEND_URL")
        or "http://localhost:5173"
    )

    normalized_base_url = base_url.rstrip("/")
    query = urlencode({"actionId": action_id})

    return f"{normalized_base_url}/?{query}"
