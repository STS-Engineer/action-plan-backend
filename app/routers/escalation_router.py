from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.models.user import User
from app.services.action_escalation_notification_service import (
    list_my_escalations_service,
    update_escalation_status_service,
)
from app.services.auth_service import get_current_user


router = APIRouter(prefix="/api/escalations", tags=["Escalations"])


@router.get("/my")
async def getMyEscalations(
    all: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return list_my_escalations_service(db, current_user, include_all=all)


@router.post("/{notification_id}/seen")
async def markEscalationSeen(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return update_escalation_status_service(db, notification_id, "seen", current_user)


@router.post("/{notification_id}/dismiss")
async def dismissEscalation(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return update_escalation_status_service(db, notification_id, "dismissed", current_user)


@router.post("/{notification_id}/resolve")
async def resolveEscalation(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return update_escalation_status_service(db, notification_id, "resolved", current_user)
