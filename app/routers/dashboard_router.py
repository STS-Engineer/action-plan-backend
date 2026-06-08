from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.config.directory_database import get_directory_db
from app.models.user import User
from app.services.auth_service import get_current_user
from app.services.auth_service import is_admin, normalize_user_role
from app.services.dashboard_service import (
    get_dashboard_drilldown_service,
    get_dashboard_overview_service,
)
from app.services.directory_service import normalize_email


router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])


@router.get("/overview")
async def getDashboardOverview(
    email: str | None = Query(None),
    scope: str = Query("my"),
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
    current_user: User = Depends(get_current_user),
):
    normalized_scope = (scope or "my").strip().lower()
    requested_email = normalize_email(email) if email else None
    token_email = normalize_email(current_user.email)

    if normalized_scope in {"all", "global"} and not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Administrator access required.")

    if not is_admin(current_user) and requested_email and requested_email != token_email:
        raise HTTPException(status_code=403, detail="Forbidden")

    return await get_dashboard_overview_service(
        db=db,
        directory_db=directory_db,
        email=requested_email or token_email,
        scope=scope,
        user_role=normalize_user_role(current_user.role),
    )


@router.get("/drilldown")
async def getDashboardDrilldown(
    email: str = Query(...),
    scope: str = Query("global"),
    chart: str = Query(...),
    bucket: str = Query(...),
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
    current_user: User = Depends(get_current_user),
):
    requested_email = normalize_email(email)
    token_email = normalize_email(current_user.email)

    normalized_scope = (scope or "my").strip().lower()

    if normalized_scope in {"all", "global"} and not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Administrator access required.")

    if not is_admin(current_user) and (not requested_email or requested_email != token_email):
        raise HTTPException(status_code=403, detail="Forbidden")

    return await get_dashboard_drilldown_service(
        db=db,
        directory_db=directory_db,
        email=requested_email or token_email,
        scope=scope,
        chart=chart,
        bucket=bucket,
        user_role=normalize_user_role(current_user.role),
    )
