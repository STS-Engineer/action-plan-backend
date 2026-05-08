from sqlalchemy import BigInteger, Column, Text, Integer, DateTime
from sqlalchemy.orm import declarative_base

DirectoryBase = declarative_base()


class CompanyMember(DirectoryBase):
    __tablename__ = "company_members"

    id = Column(BigInteger, primary_key=True)
    display_name = Column(Text)
    first_name = Column(Text)
    last_name = Column(Text)
    email = Column(Text)
    job_title = Column(Text)
    department = Column(Text)
    site = Column(Text)
    country = Column(Text)
    manager_id = Column(BigInteger)
    manager_email = Column(Text)
    depth = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    synced_at = Column(DateTime)