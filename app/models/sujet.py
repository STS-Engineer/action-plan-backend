import datetime
from app.config.database import Base
from sqlalchemy import Column, BigInteger, ForeignKey, Integer, Text, DateTime, func
from sqlalchemy.orm import relationship


class Sujet(Base):
    __tablename__ = "sujet"

    id = Column(BigInteger, primary_key=True, index=True)
    code = Column(Text, nullable=False)
    titre= Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    parent_sujet_id = Column(BigInteger, ForeignKey("sujet.id"), nullable=True)
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
    inserted_by = Column(Text, nullable=False)

    parent = relationship("Sujet", remote_side=[id], back_populates="children")
    children = relationship("Sujet", back_populates="parent")

    actions = relationship("Action", back_populates="sujet")
    