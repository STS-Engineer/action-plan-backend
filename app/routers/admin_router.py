from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.models.user import User
from app.schema.authSchema import AdminPromoteUserSchema
from app.services.auth_service import normalize_email, require_admin_user


router = APIRouter(prefix="/api/admin", tags=["Admin"])


@router.post("/promote-user")
async def promoteUser(
    payload: AdminPromoteUserSchema,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_user),
):
    email = normalize_email(str(payload.email))
    user = db.query(User).filter(User.email == email).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    old_role = user.role
    user.role = payload.role
    db.commit()
    db.refresh(user)

    return {
        "updated": True,
        "email": user.email,
        "old_role": old_role,
        "role": user.role,
        "updated_by": current_user.email,
    }
