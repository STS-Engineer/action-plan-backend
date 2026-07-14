from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.config.database import get_db
from app.models.user import User
from app.services.sujet_service import (
    delete_sujet_service,
    getSujetsService,
    getSujetsRacineService,
    get_home_summary_service,
    get_sous_sujets_by_sujet_id_service,
    get_team_sujets_racine_service,
)
from app.config.directory_database import get_directory_db
from app.config.organisation_database import get_organisation_db
from app.services.action_access_service import normalize_access_email
from app.services.auth_service import (
    get_current_user,
    is_admin,
    normalize_user_role,
    require_admin_user,
)

router = APIRouter(prefix="/api/action_plan_sujet", tags=["Sujet Action Plan"])

HomeScope = Literal["my", "team", "requested_by_me", "all"]


def validate_home_scope_request(
    email: str | None,
    scope: str | None,
    current_user: User,
) -> str | None:
    token_email = normalize_access_email(current_user.email)
    requested_email = normalize_access_email(email)

    if is_admin(current_user):
        return requested_email or token_email

    if (scope or "my").strip().lower() == "all":
        raise HTTPException(status_code=403, detail="Administrator access required.")

    if not requested_email or requested_email != token_email:
        raise HTTPException(status_code=403, detail="Forbidden")

    return requested_email

@router.get("/health")
async def health_check():
    return {"status": "ok", "message": "Action Plan API is healthy"}


@router.get("/home-summary")
async def getHomeSummary(
    email: str,
    scope: HomeScope = "my",
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
    organisation_db: Session | None = Depends(get_organisation_db),
    current_user: User = Depends(get_current_user),
):
    requested_email = validate_home_scope_request(email, scope, current_user)

    return await get_home_summary_service(
        requested_email,
        scope,
        db,
        directory_db,
        user_role=normalize_user_role(current_user.role),
        organisation_db=organisation_db,
    )

@router.get("/sujets")
async def getSujets(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_user),
):
    return await getSujetsService(db)

@router.get("/sujets/{sujet_id}")
async def getSujetById(
    sujet_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin_user),
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
    scope: HomeScope = "my",
    directory_db: Session = Depends(get_directory_db),
    organisation_db: Session | None = Depends(get_organisation_db),
    current_user: User = Depends(get_current_user),
):
    requested_email = validate_home_scope_request(email, scope, current_user)

    return await getSujetsRacineService(
        db,
        requested_email,
        status,
        scope,
        directory_db,
        user_role=normalize_user_role(current_user.role),
        organisation_db=organisation_db,
    )

@router.get("/sujets/{sujet_id}/sous-sujets")
async def getSousSujetsBySujetId(
    sujet_id: int,
    email: str | None = None,
    status: str | None = None,
    scope: HomeScope | None = None,
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
    organisation_db: Session | None = Depends(get_organisation_db),
    current_user: User = Depends(get_current_user),
):
    requested_email = validate_home_scope_request(email, scope, current_user)

    return await get_sous_sujets_by_sujet_id_service(
        sujet_id=sujet_id,
        db=db,
        email=requested_email,
        scope=scope,
        directory_db=directory_db,
        status=status,
        user_role=normalize_user_role(current_user.role),
        organisation_db=organisation_db,
    )


@router.delete("/sujets/{sujet_id}")
async def deleteSujet(
    sujet_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return await delete_sujet_service(
        sujet_id=sujet_id,
        db=db,
        current_user=current_user,
    )


@router.get("/team-sujets-racine")
async def getTeamSujetsRacine(
    email: str,
    status: str | None = None,
    db: Session = Depends(get_db),
    organisation_db: Session | None = Depends(get_organisation_db),
    current_user: User = Depends(get_current_user),
):
    requested_email = validate_home_scope_request(email, "team", current_user)

    return await get_team_sujets_racine_service(
        requested_email,
        db,
        organisation_db,
        status,
        user_role=normalize_user_role(current_user.role),
    )
