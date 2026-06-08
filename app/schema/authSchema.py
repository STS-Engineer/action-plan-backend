from typing import Literal

from pydantic import BaseModel, EmailStr


class RegisterSchema(BaseModel):
    email: EmailStr
    password: str


class LoginSchema(BaseModel):
    email: EmailStr
    password: str


class RefreshTokenSchema(BaseModel):
    refresh_token: str


class TokenResponseSchema(BaseModel):
    access_token: str
    refresh_token: str | None = None
    token_type: str = "bearer"
    expires_in: int


class AdminPromoteUserSchema(BaseModel):
    email: EmailStr
    role: Literal["user", "manager", "admin"] = "admin"
