from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.config.directory_database import get_directory_db
from app.schema.authSchema import RegisterSchema, LoginSchema
from app.services.auth_service import register_user_service, login_user_service
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
import os
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
@router.get("/me")
async def me(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
    db: Session = Depends(get_db),
):
    try:
        payload = jwt.decode(
            credentials.credentials,
            os.getenv("JWT_SECRET_KEY"),
            algorithms=[os.getenv("JWT_ALGORITHM", "HS256")]
        )

        email = payload.get("sub")

        if not email:
            raise HTTPException(status_code=401, detail="Invalid token.")

        user = db.query(User).filter(User.email == email).first()

        if not user:
            raise HTTPException(status_code=401, detail="User not found.")

        return {
            "id": user.id,
            "email": user.email,
            "full_name": user.full_name,
            "role": user.role,
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")