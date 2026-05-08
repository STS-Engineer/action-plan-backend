from app.models.action_event_log import ActionEventLog


def log_action_event(
    db,
    action_id: int,
    event_type: str,
    old_value: str | None = None,
    new_value: str | None = None,
    details: str | None = None,
    created_by: str | None = None,
):
    event = ActionEventLog(
        action_id=action_id,
        event_type=event_type,
        old_value=old_value,
        new_value=new_value,
        details=details,
        created_by=created_by,
    )

    db.add(event)
    return event