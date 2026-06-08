import datetime
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.action_status_comment import ActionStatusComment
from app.services.action_priority_service import enrich_action_priority
from app.services.action_status_logic_service import (
    CLOSED_HOME_BUCKET,
    OVERDUE_STATUSES,
    get_action_active_predicate,
    get_normalized_action_status_expression,
)


async def update_overdue_actions_service(db: Session):
    today = datetime.date.today()

    actions = (
        db.query(Action)
        .filter(get_action_active_predicate(Action))
        .filter(Action.due_date.isnot(None))
        .filter(Action.due_date < today)
        .filter(
            ~get_normalized_action_status_expression(Action).in_(
                [CLOSED_HOME_BUCKET, *OVERDUE_STATUSES]
            )
        )
        .all()
    )

    updated_count = 0

    for action in actions:
        old_status = action.status

        action.status = "overdue"

        enrich_action_priority(action)

        status_comment = ActionStatusComment(
            action_id=action.id,
            old_status=old_status,
            new_status="overdue",
            comment="Status automatically changed to overdue because due date has passed.",
            created_by="system",
        )

        db.add(status_comment)
        updated_count += 1

    db.commit()

    return {
        "message": "Overdue actions updated successfully",
        "updated_actions": updated_count,
    }
