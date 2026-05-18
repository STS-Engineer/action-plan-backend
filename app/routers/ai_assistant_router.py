from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.database import get_db
from app.config.directory_database import get_directory_db
from app.schemas.ai_action_plan_schema import AIActionPlanDraftRequest, PlanV1
from app.services.ai_action_plan_service import (
    create_action_plan_service,
    generate_action_plan_draft_service,
)


router = APIRouter(prefix="/api/ai", tags=["AI Assistant"])


@router.post("/action-plan/draft", response_model=PlanV1)
async def draft_action_plan(
    payload: AIActionPlanDraftRequest,
    directory_db: Session = Depends(get_directory_db),
):
    return await generate_action_plan_draft_service(payload, directory_db)


@router.post("/action-plan/create")
async def create_action_plan(
    payload: PlanV1,
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
):
    return await create_action_plan_service(payload, db, directory_db)
