from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.directory_database import get_directory_db
from app.services.directory_service import (
    get_member_by_email,
    get_direct_reports,
    get_all_underlings,
    get_manager_chain,
)

router = APIRouter(prefix="/api/directory", tags=["Directory"])


def member_to_dict(member):
    return {
        "id": member.id,
        "display_name": member.display_name,
        "email": member.email,
        "job_title": member.job_title,
        "department": member.department,
        "site": member.site,
        "country": member.country,
        "manager_email": member.manager_email,
        "depth": member.depth,
    }


@router.get("/member")
async def getMember(
    email: str,
    db: Session = Depends(get_directory_db),
):
    member = get_member_by_email(db, email)

    if not member:
        return {"found": False}

    return {
        "found": True,
        "member": member_to_dict(member),
    }


@router.get("/direct-reports")
async def getDirectReports(
    manager_email: str,
    db: Session = Depends(get_directory_db),
):
    reports = get_direct_reports(db, manager_email)

    return {
        "count": len(reports),
        "reports": [member_to_dict(member) for member in reports],
    }


@router.get("/underlings")
async def getUnderlings(
    manager_email: str,
    db: Session = Depends(get_directory_db),
):
    underlings = get_all_underlings(db, manager_email)

    return {
        "count": len(underlings),
        "underlings": [member_to_dict(member) for member in underlings],
    }


@router.get("/manager-chain")
async def getManagerChain(
    email: str,
    db: Session = Depends(get_directory_db),
):
    chain = get_manager_chain(db, email)

    return {
        "count": len(chain),
        "chain": [member_to_dict(member) for member in chain],
    }