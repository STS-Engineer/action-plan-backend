from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.models.user import User
from app.schema.authSchema import AdminPromoteUserSchema
from app.services.action_reminder_service import (
    debug_daily_reminders_for_user_service,
    run_daily_grouped_reminders_service,
)
from app.services.auth_service import normalize_email, require_admin_user
from app.services.email_service import get_smtp_config_diagnostics, send_smtp_test_email


router = APIRouter(prefix="/api/admin", tags=["Admin"])


class DailyReminderRunRequest(BaseModel):
    dry_run: bool = True
    test_email: EmailStr | None = None


class SmtpTestRequest(BaseModel):
    to_email: EmailStr


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


@router.get("/reminders/debug-user")
async def debugUserDailyReminders(
    email: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_user),
):
    return await debug_daily_reminders_for_user_service(db, email)


@router.post("/reminders/daily/run")
async def runDailyReminders(
    payload: DailyReminderRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_user),
):
    return await run_daily_grouped_reminders_service(
        db,
        dry_run=payload.dry_run,
        test_email=str(payload.test_email) if payload.test_email else None,
    )


@router.get("/reminders/smtp-config")
async def getSmtpConfig(
    current_user: User = Depends(require_admin_user),
):
    return get_smtp_config_diagnostics()


@router.post("/reminders/smtp-test")
async def runSmtpTest(
    payload: SmtpTestRequest,
    current_user: User = Depends(require_admin_user),
):
    return send_smtp_test_email(str(payload.to_email))
