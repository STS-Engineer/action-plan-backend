import datetime
from sqlalchemy import BigInteger, Column, Text, DateTime, ForeignKey, func
from sqlalchemy.orm import relationship
from app.config.database import Base


class ActionStatusComment(Base):
    __tablename__ = "action_status_comment"

    id = Column(BigInteger, primary_key=True, index=True)
    action_id = Column(BigInteger, ForeignKey("action.id", ondelete="CASCADE"), nullable=False)
    old_status = Column(Text, nullable=True)
    new_status = Column(Text, nullable=False)
    comment = Column(Text, nullable=True)
    created_by = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        server_default=func.now(),
    )