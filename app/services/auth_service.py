import os
from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import HTTPException, status, Depends
from app.models.user import User
from app.services.directory_service import get_member_by_email
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from app.config.database import get_db

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()
ACCESS_TOKEN_USE = "access"
REFRESH_TOKEN_USE = "refresh"
SUPPORTED_USER_ROLES = {"user", "manager", "admin"}


def get_jwt_secret_key():
    secret_key = os.getenv("JWT_SECRET_KEY")

    if not secret_key:
        raise HTTPException(status_code=500, detail="JWT secret is not configured.")

    return secret_key


def get_jwt_algorithm():
    return os.getenv("JWT_ALGORITHM", "HS256")


def get_access_token_expire_minutes():
    return int(os.getenv("JWT_ACCESS_EXPIRE_MINUTES", "60"))


def get_refresh_token_expire_days():
    return int(os.getenv("JWT_REFRESH_EXPIRE_DAYS", "7"))


def get_access_token_expires_in_seconds():
    return get_access_token_expire_minutes() * 60


def normalize_email(email: str):
    return email.strip().lower()


def normalize_user_role(role: str | None) -> str:
    normalized_role = str(role or "user").strip().lower()

    if normalized_role in SUPPORTED_USER_ROLES:
        return normalized_role

    return "user"


def is_admin(user) -> bool:
    return normalize_user_role(getattr(user, "role", None)) == "admin"


def is_manager(user) -> bool:
    return normalize_user_role(getattr(user, "role", None)) in {"manager", "admin"}


def is_admin_role(role: str | None) -> bool:
    return normalize_user_role(role) == "admin"


def hash_password(password: str):
    return pwd_context.hash(password)


def verify_password(password: str, hashed_password: str):
    return pwd_context.verify(password, hashed_password)


def create_access_token(data: dict):
    secret_key = get_jwt_secret_key()
    algorithm = get_jwt_algorithm()
    expire_minutes = get_access_token_expire_minutes()

    expire = datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)

    payload = {
        **data,
        "token_use": ACCESS_TOKEN_USE,
        "exp": expire,
    }

    return jwt.encode(payload, secret_key, algorithm=algorithm)


def create_refresh_token(data: dict):
    secret_key = get_jwt_secret_key()
    algorithm = get_jwt_algorithm()
    expire_days = get_refresh_token_expire_days()

    expire = datetime.now(timezone.utc) + timedelta(days=expire_days)

    payload = {
        **data,
        "token_use": REFRESH_TOKEN_USE,
        "exp": expire,
    }

    return jwt.encode(payload, secret_key, algorithm=algorithm)


def build_token_payload(user: User):
    return {
        "sub": user.email,
        "user_id": user.id,
        "role": normalize_user_role(user.role),
    }


def build_auth_response(user: User, include_refresh_token: bool = True):
    access_token = create_access_token(build_token_payload(user))
    response = {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": get_access_token_expires_in_seconds(),
    }

    if include_refresh_token:
        response["refresh_token"] = create_refresh_token(build_token_payload(user))

    response["user"] = {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "role": normalize_user_role(user.role),
    }

    return response


def register_user_service(payload, db, directory_db):
    email = normalize_email(payload.email)

    directory_member = get_member_by_email(directory_db, email)

    if not directory_member:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email is not authorized. User must exist in AVOCarbon directory.",
        )

    existing_user = db.query(User).filter(User.email == email).first()

    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User already exists.",
        )

    user = User(
        email=email,
        full_name=directory_member.display_name,
        hashed_password=hash_password(payload.password),
        role="user",
        is_active=True,
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "role": normalize_user_role(user.role),
    }


def login_user_service(payload, db):
    email = normalize_email(payload.email)

    user = db.query(User).filter(User.email == email).first()

    if not user or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is disabled.",
        )

    return build_auth_response(user, include_refresh_token=True)


def refresh_access_token_service(payload, db):
    try:
        token_payload = jwt.decode(
            payload.refresh_token,
            get_jwt_secret_key(),
            algorithms=[get_jwt_algorithm()],
        )
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token.")

    if token_payload.get("token_use") != REFRESH_TOKEN_USE:
        raise HTTPException(status_code=401, detail="Invalid refresh token.")

    email = token_payload.get("sub")

    if not email:
        raise HTTPException(status_code=401, detail="Invalid refresh token.")

    user = db.query(User).filter(User.email == email).first()

    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="Invalid refresh token.")

    return build_auth_response(user, include_refresh_token=False)


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    token = credentials.credentials

    try:
        payload = jwt.decode(
            token,
            get_jwt_secret_key(),
            algorithms=[get_jwt_algorithm()]
        )

        if payload.get("token_use") == REFRESH_TOKEN_USE:
            raise HTTPException(status_code=401, detail="Invalid access token.")

        email = payload.get("sub")

        if not email:
            raise HTTPException(status_code=401, detail="Invalid token.")

        user = db.query(User).filter(User.email == email).first()

        if not user:
            raise HTTPException(status_code=401, detail="User not found.")

        return user

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")


def require_admin_user(current_user: User = Depends(get_current_user)):
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Administrator access required.")

    return current_user
