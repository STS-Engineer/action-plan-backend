from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.config.directory_database import get_directory_db
from app.schema.authSchema import LoginSchema, RefreshTokenSchema, RegisterSchema
from app.services.auth_service import (
    get_current_user,
    login_user_service,
    refresh_access_token_service,
    register_user_service,
)
from app.models.user import User
router = APIRouter(prefix="/api/auth", tags=["Auth"])


@router.post("/register")
async def register(
    payload: RegisterSchema,
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
):
    return register_user_service(payload, db, directory_db)


@router.post("/login")
async def login(
    payload: LoginSchema,
    db: Session = Depends(get_db),
):
    return login_user_service(payload, db)


@router.post("/refresh")
async def refresh(
    payload: RefreshTokenSchema,
    db: Session = Depends(get_db),
):
    return refresh_access_token_service(payload, db)


@router.get("/me")
async def me(current_user: User = Depends(get_current_user)):
    return {
        "id": current_user.id,
        "email": current_user.email,
        "full_name": current_user.full_name,
        "role": current_user.role,
    }
