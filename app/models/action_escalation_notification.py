import datetime

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.config.database import Base


class ActionEscalationNotification(Base):
    __tablename__ = "action_escalation_notification"

    id = Column(BigInteger, primary_key=True, index=True)
    action_id = Column(BigInteger, ForeignKey("action.id", ondelete="CASCADE"), nullable=False, index=True)
    recipient_email = Column(Text, nullable=False, index=True)
    cc_emails = Column(JSONB, nullable=True)
    escalation_level = Column(Integer, nullable=False)
    hierarchy_source_used = Column(Text, nullable=False)
    responsible_chain = Column(JSONB, nullable=True)
    requester_chain = Column(JSONB, nullable=True)
    status = Column(Text, nullable=False, default="pending", server_default="pending", index=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        server_default=func.now(),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        onupdate=lambda: datetime.datetime.now(datetime.timezone.utc),
        server_default=func.now(),
    )
    seen_at = Column(DateTime(timezone=True), nullable=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    last_summary_email_sent_at = Column(DateTime(timezone=True), nullable=True)

    action = relationship("Action")
