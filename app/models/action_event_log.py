import datetime
from app.config.database import Base
from sqlalchemy import BigInteger, Column, Text, DateTime, ForeignKey, func


class ActionEventLog(Base):
    __tablename__ = "action_event_log"

    id = Column(BigInteger, primary_key=True, index=True)
    action_id = Column(BigInteger, ForeignKey("action.id", ondelete="CASCADE"), nullable=False)
    event_type = Column(Text, nullable=False)
    old_value = Column(Text, nullable=True)
    new_value = Column(Text, nullable=True)
    details = Column(Text, nullable=True)
    created_by = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        server_default=func.now(),
    )