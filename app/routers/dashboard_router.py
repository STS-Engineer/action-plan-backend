from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.config.directory_database import get_directory_db
from app.services.dashboard_service import get_dashboard_overview_service


router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])


@router.get("/overview")
async def getDashboardOverview(
    email: str | None = Query(None),
    scope: str = Query("global"),
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
):
    return await get_dashboard_overview_service(
        db=db,
        directory_db=directory_db,
        email=email,
        scope=scope,
    )
