import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

DIRECTORY_DB_USER = os.getenv("DIRECTORY_DB_USER")
DIRECTORY_DB_PASSWORD = quote_plus(os.getenv("DIRECTORY_DB_PASSWORD", ""))
DIRECTORY_DB_HOST = os.getenv("DIRECTORY_DB_HOST")
DIRECTORY_DB_PORT = os.getenv("DIRECTORY_DB_PORT", "5432")
DIRECTORY_DB_NAME = os.getenv("DIRECTORY_DB_NAME")

DIRECTORY_DATABASE_URL = (
    f"postgresql://{DIRECTORY_DB_USER}:{DIRECTORY_DB_PASSWORD}"
    f"@{DIRECTORY_DB_HOST}:{DIRECTORY_DB_PORT}/{DIRECTORY_DB_NAME}"
    f"?sslmode=require"
)

directory_engine = create_engine(DIRECTORY_DATABASE_URL)

DirectorySessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=directory_engine,
)


def get_directory_db():
    db = DirectorySessionLocal()
    try:
        yield db
    finally:
        db.close()