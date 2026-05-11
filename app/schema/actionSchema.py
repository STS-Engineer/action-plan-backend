from pydantic import BaseModel
from typing import Optional


class updateActionStatusSchema(BaseModel):
    status: str
    comment: Optional[str] = None
    created_by: Optional[str] = None