import datetime
from sqlalchemy import BigInteger, Column, Text, DateTime, ForeignKey, func
from app.config.database import Base


class ActionAttachment(Base):
    __tablename__ = "action_attachment"

    id = Column(BigInteger, primary_key=True, index=True)
    action_id = Column(BigInteger, ForeignKey("action.id", ondelete="CASCADE"), nullable=False)
    file_name = Column(Text, nullable=False)
    file_path = Column(Text, nullable=False)
    uploaded_by = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        server_default=func.now(),
    )