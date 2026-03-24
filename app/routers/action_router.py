

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.config.database import get_db
from app.services.action_Service import (
    get_actions_by_sujet_id_service,
    get_action_by_id_service,
    get_sous_actions_by_action_id_service,
    get_statistiques_service
)



router = APIRouter(prefix="/api/action_plan_action", tags=["Action Plan"])

@router.get("/sujets/{sujet_id}/actions")
async def getActionsBySujetId(
    sujet_id: int,
    db: Session = Depends(get_db)
):
    return await get_actions_by_sujet_id_service(sujet_id, db)

@router.get("/actions/{action_id}")
async def getActionById(
    action_id: int,
    db: Session = Depends(get_db)
):
    return await get_action_by_id_service(action_id, db)

@router.get("/actions/{action_id}/sous-actions")
async def getSousActionsByActionId(
    action_id: int,
    db: Session = Depends(get_db)
):
    return await get_sous_actions_by_action_id_service(action_id, db)

@router.get("/statistiques")
async def get_statistiques(
    db: Session = Depends(get_db)
):
    return await get_statistiques_service(db)