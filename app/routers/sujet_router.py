from typing import Literal

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.config.database import get_db
from app.services.sujet_service import (
    getSujetsService,
    getSujetsRacineService,
    get_home_summary_service,
    get_sous_sujets_by_sujet_id_service,
    get_team_sujets_racine_service,
)
from app.config.directory_database import get_directory_db

router = APIRouter(prefix="/api/action_plan_sujet", tags=["Sujet Action Plan"])

@router.get("/health")
async def health_check():
    return {"status": "ok", "message": "Action Plan API is healthy"}


@router.get("/home-summary")
async def getHomeSummary(
    email: str,
    scope: Literal["my", "team"] = "my",
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
):
    return await get_home_summary_service(email, scope, db, directory_db)

@router.get("/sujets")
async def getSujets(
    db: Session = Depends(get_db)
):
    return await getSujetsService(db)

@router.get("/sujets/{sujet_id}")
async def getSujetById(
    sujet_id: int,
    db: Session = Depends(get_db)
):
    sujets = await getSujetsService(db)
    for sujet in sujets:
        if sujet["id"] == sujet_id:
            return sujet
    return {"error": "Sujet not found"}


@router.get("/sujets-racine")
async def getSujetsRacine(
    db: Session = Depends(get_db),
    email: str | None = None,
    status: str | None = None,
):
    return await getSujetsRacineService(db, email, status)

@router.get("/sujets/{sujet_id}/sous-sujets")
async def getSousSujetsBySujetId(
    sujet_id: int,
    db: Session = Depends(get_db)
):
    return await get_sous_sujets_by_sujet_id_service(sujet_id, db)
@router.get("/team-sujets-racine")
async def getTeamSujetsRacine(
    email: str,
    status: str | None = None,
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
):
    return await get_team_sujets_racine_service(email, db, directory_db, status)
