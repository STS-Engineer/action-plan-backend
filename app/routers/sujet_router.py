from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.config.database import get_db
from app.services.sujet_service import getSujetsService, getSujetsRacineService, get_sous_sujets_by_sujet_id_service


router = APIRouter(prefix="/api/action_plan_sujet", tags=["Sujet Action Plan"])

@router.get("/health")
async def health_check():
    return {"status": "ok", "message": "Action Plan API is healthy"}

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
    db: Session = Depends(get_db)
):
    return await getSujetsRacineService(db)

@router.get("/sujets/{sujet_id}/sous-sujets")
async def getSousSujetsBySujetId(
    sujet_id: int,
    db: Session = Depends(get_db)
):
    return await get_sous_sujets_by_sujet_id_service(sujet_id, db)
