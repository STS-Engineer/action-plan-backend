import logging
import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


load_dotenv()

logger = logging.getLogger(__name__)


def _build_organisation_database_url() -> str | None:
    explicit_url = os.getenv("ORGANISATION_DATABASE_URL")
    if explicit_url:
        return explicit_url

    user = os.getenv("ORGANISATION_DB_USER")
    password = os.getenv("ORGANISATION_DB_PASSWORD")
    host = os.getenv("ORGANISATION_DB_HOST")
    port = os.getenv("ORGANISATION_DB_PORT", "5432")
    name = os.getenv("ORGANISATION_DB_NAME")

    if not (user and host and name):
        return None

    encoded_password = quote_plus(password or "")
    return (
        f"postgresql+psycopg2://{user}:{encoded_password}"
        f"@{host}:{port}/{name}"
        f"?sslmode=require"
    )


ORGANISATION_DATABASE_URL = _build_organisation_database_url()

organisation_engine = (
    create_engine(
        ORGANISATION_DATABASE_URL,
        pool_pre_ping=True,
    )
    if ORGANISATION_DATABASE_URL
    else None
)

OrganisationSessionLocal = (
    sessionmaker(autocommit=False, autoflush=False, bind=organisation_engine)
    if organisation_engine is not None
    else None
)


def is_organisation_db_configured() -> bool:
    return organisation_engine is not None


def get_organisation_db():
    if OrganisationSessionLocal is None:
        yield None
        return

    db = OrganisationSessionLocal()
    try:
        yield db
    finally:
        db.close()
