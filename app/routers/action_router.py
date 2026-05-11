
from fastapi.responses import HTMLResponse
from fastapi import APIRouter, Depends, Query, UploadFile, File, Form
from sqlalchemy.orm import Session
from app.config.database import get_db
from app.schema.actionSchema import updateActionStatusSchema
from app.services.action_Service import (
    get_actions_by_sujet_id_service,
    get_action_by_id_service,
    get_sous_actions_by_action_id_service,
    get_statistiques_service,
    get_emails_service,
    get_my_actions_service,
    get_team_actions_service,
    update_action_status_service,
    mark_action_closed_from_email_service
)
from app.services.action_priority_service import recalculate_all_action_priorities_service
from app.services.action_reminder_service import (
    send_test_due_date_reminders_service,
    send_grouped_due_date_reminders_service
)
from app.services.weekly_report_service import (
    send_test_weekly_responsable_reports_service,
    send_weekly_responsable_reports_service,
)
from app.services.action_search_service import (
    search_actions_service,
    
)
from app.services.action_attachment_service import (
    upload_action_attachment_service,
    get_action_attachments_service,
)
from app.config.directory_database import get_directory_db
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

@router.get("/emails")
async def getEmails(
    db: Session = Depends(get_db)
):
    return await get_emails_service(db)

@router.put("/actions/{action_id}/status")
async def updateActionStatus(
    action_id: int,
    payload: updateActionStatusSchema,
    db: Session = Depends(get_db)
):
    return await update_action_status_service(
        action_id=action_id,
        status=payload.status,
        db=db,
        comment=payload.comment,
        created_by=payload.created_by,
    )
@router.post("/recalculate-priorities")
async def recalculatePriorities(
    db: Session = Depends(get_db)
):
    return await recalculate_all_action_priorities_service(db)
@router.post("/send-due-date-reminders")
async def sendDueDateReminders(
    db: Session = Depends(get_db)
):
    return await send_due_date_reminders_service(db)
@router.post("/send-test-due-date-reminders")
async def sendTestDueDateReminders(
    test_email: str = Query(...),
    db: Session = Depends(get_db)
):
    return await send_test_due_date_reminders_service(db, test_email)
@router.post("/send-grouped-due-date-reminders")
async def sendGroupedDueDateReminders(
    db: Session = Depends(get_db)
):
    return await send_grouped_due_date_reminders_service(db)
@router.get("/actions/{action_id}/mark-closed-from-email", response_class=HTMLResponse)
async def markActionClosedFromEmail(
    action_id: int,
    db: Session = Depends(get_db)
):
    return await mark_action_closed_from_email_service(action_id, db)
@router.post("/send-test-weekly-reports")
async def sendTestWeeklyReports(
    test_email: str = Query(...),
    db: Session = Depends(get_db)
):
    return await send_test_weekly_responsable_reports_service(db, test_email)


@router.post("/send-weekly-reports")
async def sendWeeklyReports(
    db: Session = Depends(get_db)
):
    return await send_weekly_responsable_reports_service(db)
@router.get("/search")
async def searchActions(
    query: str,
    db: Session = Depends(get_db)
):
    return await search_actions_service(query, db)
@router.get("/my-actions")
async def getMyActions(
    email: str,
    db: Session = Depends(get_db),
):
    return await get_my_actions_service(email, db)


@router.get("/team-actions")
async def getTeamActions(
    email: str,
    db: Session = Depends(get_db),
    directory_db: Session = Depends(get_directory_db),
):
    return await get_team_actions_service(email, db, directory_db)
@router.post("/actions/{action_id}/attachments")
async def uploadActionAttachment(
    action_id: int,
    file: UploadFile = File(...),
    uploaded_by: str | None = Form(None),
    db: Session = Depends(get_db),
):
    return await upload_action_attachment_service(
        action_id=action_id,
        file=file,
        db=db,
        uploaded_by=uploaded_by,
    )


@router.get("/actions/{action_id}/attachments")
async def getActionAttachments(
    action_id: int,
    db: Session = Depends(get_db),
):
    return await get_action_attachments_service(action_id, db)