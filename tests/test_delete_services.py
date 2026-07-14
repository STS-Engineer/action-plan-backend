import asyncio
import datetime
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.database import Base
from app.models.action import Action
from app.models.action_event_log import ActionEventLog
from app.models.sujet import Sujet
from app.services.action_Service import delete_action_service
from app.services.sujet_service import delete_sujet_service


def user(email="authenticated@example.com", role="user"):
    return SimpleNamespace(email=email, role=role)


class DeleteServiceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.db = self.SessionLocal()

        @event.listens_for(self.db, "before_flush")
        def assign_sqlite_bigint_ids(session, _flush_context, _instances):
            next_id = 1
            for item in session.new:
                if isinstance(item, ActionEventLog) and item.id is None:
                    item.id = next_id
                    next_id += 1

    def tearDown(self):
        self.db.close()
        Base.metadata.drop_all(bind=self.engine)

    def add_sujet(self, *, id, titre="Topic", parent_sujet_id=None):
        sujet = Sujet(
            id=id,
            code=f"S{id}",
            titre=titre,
            description=None,
            parent_sujet_id=parent_sujet_id,
            inserted_by="test",
        )
        self.db.add(sujet)
        self.db.commit()
        return sujet

    def add_action(self, *, id, sujet_id, title="Action", owner="owner@example.com"):
        action = Action(
            id=id,
            sujet_id=sujet_id,
            type="action",
            titre=title,
            description=None,
            status="open",
            email_responsable=owner,
            due_date=datetime.date(2026, 7, 20),
        )
        self.db.add(action)
        self.db.commit()
        return action

    def test_authenticated_user_can_soft_delete_action_without_ownership(self):
        self.add_sujet(id=1)
        action = self.add_action(id=10, sujet_id=1, owner="someone-else@example.com")

        result = asyncio.run(
            delete_action_service(
                action_id=action.id,
                db=self.db,
                directory_db=None,
                current_user=user("deleter@example.com"),
            )
        )

        deleted_action = self.db.query(Action).filter(Action.id == action.id).one()
        event = self.db.query(ActionEventLog).filter(ActionEventLog.action_id == action.id).one()

        self.assertTrue(result["deleted"])
        self.assertTrue(deleted_action.is_deleted)
        self.assertEqual(deleted_action.deleted_by, "deleter@example.com")
        self.assertIsNotNone(deleted_action.deleted_at)
        self.assertEqual(event.event_type, "action_archived")

    def test_empty_sujet_soft_delete_sets_audit_fields(self):
        sujet = self.add_sujet(id=2, titre="Empty topic")

        result = asyncio.run(
            delete_sujet_service(
                sujet_id=sujet.id,
                db=self.db,
                current_user=user("deleter@example.com"),
            )
        )

        deleted_sujet = self.db.query(Sujet).filter(Sujet.id == sujet.id).one()

        self.assertTrue(result["deleted"])
        self.assertTrue(deleted_sujet.is_deleted)
        self.assertEqual(deleted_sujet.deleted_by, "deleter@example.com")
        self.assertIsNotNone(deleted_sujet.deleted_at)

    def test_sujet_with_active_actions_cannot_be_deleted(self):
        sujet = self.add_sujet(id=3, titre="Topic with action")
        self.add_action(id=30, sujet_id=sujet.id)

        with self.assertRaises(HTTPException) as error:
            asyncio.run(
                delete_sujet_service(
                    sujet_id=sujet.id,
                    db=self.db,
                    current_user=user(),
                )
            )

        self.assertEqual(error.exception.status_code, 409)
        self.assertIn("active actions", error.exception.detail)
        self.assertFalse(self.db.query(Sujet).filter(Sujet.id == sujet.id).one().is_deleted)

    def test_sujet_with_active_child_sujets_cannot_be_deleted(self):
        parent = self.add_sujet(id=4, titre="Parent")
        self.add_sujet(id=5, titre="Child", parent_sujet_id=parent.id)

        with self.assertRaises(HTTPException) as error:
            asyncio.run(
                delete_sujet_service(
                    sujet_id=parent.id,
                    db=self.db,
                    current_user=user(),
                )
            )

        self.assertEqual(error.exception.status_code, 409)
        self.assertIn("child topics", error.exception.detail)
        self.assertFalse(self.db.query(Sujet).filter(Sujet.id == parent.id).one().is_deleted)


if __name__ == "__main__":
    unittest.main()
