import datetime
from app.config.database import Base
from sqlalchemy import BigInteger, Date, ForeignKey, Column, Text, DateTime, func, Integer
from sqlalchemy.orm import relationship

class Action(Base):
    __tablename__ = "action"

    id = Column(BigInteger, primary_key=True, index=True)
    sujet_id = Column(Integer, ForeignKey("sujet.id", ondelete="CASCADE"), nullable=False)
    parent_action_id = Column(BigInteger, ForeignKey("action.id", ondelete="CASCADE"), nullable=True)
    type = Column(Text, nullable=False)
    titre = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    status = Column(Text, nullable=True, default="open")
    priorite = Column(Integer, nullable=True)
    responsable = Column(Text, nullable=True)
    email_responsable = Column(Text, nullable=True)
    due_date = Column(Date(), nullable=True)
    ordre = Column(Integer, nullable=True)
    depth = Column(Integer, nullable=True)
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
    closed_date = Column(Date(), nullable=True)
    sujet = relationship("Sujet", back_populates="actions")

    parent = relationship(
        "Action",
        remote_side=[id],
        back_populates="children",
        foreign_keys=[parent_action_id],
    )

    children = relationship(
        "Action",
        back_populates="parent",
        foreign_keys=[parent_action_id],
        passive_deletes=True,
    )