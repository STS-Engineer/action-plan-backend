import datetime
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.models.action_escalation_notification import ActionEscalationNotification
from app.models.action_event_log import ActionEventLog
from app.services.action_escalation_notification_service import (
    _dedupe_current_work_queue_notifications,
    update_escalation_status_service,
)
from app.services.action_escalation_service import _upsert_pending_notification


class FakeQuery:
    def __init__(self, first_result=None, all_result=None):
        self._first_result = first_result
        self._all_result = all_result or []

    def filter(self, *args, **kwargs):
        return self

    def options(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return self._first_result

    def all(self):
        return self._all_result


class FakeDb:
    def __init__(self, queries):
        self.queries = list(queries)
        self.added = []
        self.committed = False
        self.refreshed = []

    def query(self, *args, **kwargs):
        return self.queries.pop(0)

    def add(self, item):
        if isinstance(item, ActionEscalationNotification) and item.id is None:
            item.id = 100 + len([
                existing
                for existing in self.added
                if isinstance(existing, ActionEscalationNotification)
            ])
        self.added.append(item)

    def flush(self):
        for index, item in enumerate(self.added, start=100):
            if isinstance(item, ActionEscalationNotification) and item.id is None:
                item.id = index

    def commit(self):
        self.committed = True

    def refresh(self, item):
        self.refreshed.append(item)


def notification(
    *,
    id,
    action_id=1,
    recipient_email="olivier.spicker@avocarbon.com",
    level=10,
    status="pending",
    created_at=None,
    updated_at=None,
):
    item = ActionEscalationNotification(
        action_id=action_id,
        recipient_email=recipient_email,
        escalation_level=level,
        status=status,
    )
    item.id = id
    item.created_at = created_at or datetime.datetime(2026, 7, 10, tzinfo=datetime.timezone.utc)
    item.updated_at = updated_at or item.created_at
    return item


def action(**overrides):
    values = {
        "id": 1,
        "priority_index": 10,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def resolution(recipient="olivier.spicker@avocarbon.com", level=12):
    return {
        "to_email": recipient,
        "cc_emails": ["manager@example.com"],
        "level": level,
        "responsible_chain": {"chain": []},
        "requester_chain": {"chain": []},
    }


def test_upsert_reuses_same_action_recipient_pending_notification_and_updates_level():
    existing = notification(id=10, level=10)
    db = FakeDb([
        FakeQuery(first_result=existing),
        FakeQuery(all_result=[]),
        FakeQuery(all_result=[]),
    ])

    result, created = _upsert_pending_notification(db, action(), resolution(level=12))

    assert result is existing
    assert created is False
    assert existing.escalation_level == 12
    assert existing.cc_emails == ["manager@example.com"]
    assert existing.hierarchy_source_used == "v_people_with_boss"
    assert not any(isinstance(item, ActionEscalationNotification) for item in db.added)


def test_upsert_dismisses_old_recipient_pending_row_when_recipient_changes():
    old_recipient = notification(id=11, recipient_email="old.manager@example.com", level=5)
    db = FakeDb([
        FakeQuery(first_result=None),
        FakeQuery(all_result=[old_recipient]),
        FakeQuery(all_result=[]),
    ])

    result, created = _upsert_pending_notification(
        db,
        action(),
        resolution(recipient="new.manager@example.com", level=6),
    )

    assert created is True
    assert result.recipient_email == "new.manager@example.com"
    assert result.escalation_level == 6
    assert old_recipient.status == "dismissed"
    assert any(
        isinstance(item, ActionEventLog)
        and item.event_type == "action_escalation_recipient_changed"
        for item in db.added
    )


def test_upsert_dismisses_duplicate_same_recipient_pending_rows():
    current = notification(id=12, level=10)
    duplicate = notification(id=13, level=9)
    db = FakeDb([
        FakeQuery(first_result=current),
        FakeQuery(all_result=[]),
        FakeQuery(all_result=[duplicate]),
    ])

    result, created = _upsert_pending_notification(db, action(), resolution(level=11))

    assert result is current
    assert created is False
    assert current.status == "pending"
    assert current.escalation_level == 11
    assert duplicate.status == "dismissed"
    assert any(
        isinstance(item, ActionEventLog)
        and item.event_type == "action_escalation_duplicate_pending_dismissed"
        for item in db.added
    )


def test_work_queue_dedupe_selects_highest_level_then_latest_row():
    older = notification(id=1, level=10)
    middle = notification(id=2, level=11)
    latest = notification(id=3, level=12)
    other_recipient = notification(id=4, recipient_email="other@example.com", level=1)
    for item in [older, middle, latest, other_recipient]:
        item.action = action(priority_index=1)

    result = _dedupe_current_work_queue_notifications([
        older,
        middle,
        latest,
        other_recipient,
    ])

    assert [item.id for item in result] == [3, 4]


def test_dismiss_visible_notification_transitions_hidden_pending_group_rows():
    visible = notification(id=3, level=12)
    hidden = notification(id=2, level=11)
    db = FakeDb([
        FakeQuery(first_result=visible),
        FakeQuery(all_result=[visible, hidden]),
    ])

    response = update_escalation_status_service(
        db,
        notification_id=visible.id,
        status="dismissed",
        current_user=SimpleNamespace(email="manager@example.com", role="admin"),
    )

    assert response["updated"] is True
    assert visible.status == "dismissed"
    assert hidden.status == "dismissed"
    assert db.committed is True
    assert sum(
        1
        for item in db.added
        if isinstance(item, ActionEventLog)
        and item.event_type == "action_escalation_dismissed"
    ) == 2


if __name__ == "__main__":
    for name, test in sorted(globals().items()):
        if name.startswith("test_") and callable(test):
            test()
    print("action escalation lifecycle tests passed")
