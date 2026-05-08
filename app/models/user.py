import datetime
from sqlalchemy import BigInteger, Column, Text, DateTime, Boolean, func
from app.config.database import Base


class User(Base):
    __tablename__ = "app_user"

    id = Column(BigInteger, primary_key=True, index=True)
    email = Column(Text, nullable=False, unique=True, index=True)
    full_name = Column(Text, nullable=True)
    hashed_password = Column(Text, nullable=False)
    role = Column(Text, nullable=True, default="user")
    is_active = Column(Boolean, nullable=True, default=True)

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