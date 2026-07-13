from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from app.models.action import Action  # noqa: F401
from app.models.sujet import Sujet


KPI_FORM_SOURCE_APPLICATION = "KPI Form"
APQP_SOURCE_APPLICATION = "APQP Sprint Board"


def _kpi_form_predicate():
    return Sujet.description.ilike("%Link key: kpi-form|%")


def _apqp_predicate():
    return or_(
        Sujet.code.like("APQP-%"),
        Sujet.inserted_by == "apqp-app",
    )


def _null_source_predicate():
    return Sujet.source_application.is_(None)


def _count_rows(db: Session, predicate) -> int:
    return int(
        db.query(func.count(Sujet.id))
        .filter(predicate)
        .scalar()
        or 0
    )


def _sample_rows(db: Session, predicate, limit: int = 10) -> list[dict]:
    rows = (
        db.query(Sujet.id, Sujet.code, Sujet.titre, Sujet.inserted_by)
        .filter(predicate)
        .order_by(Sujet.created_at.desc(), Sujet.id.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": row.id,
            "code": row.code,
            "titre": row.titre,
            "inserted_by": row.inserted_by,
        }
        for row in rows
    ]


def classify_null_sujet_source_applications(
    db: Session,
    dry_run: bool = True,
    sample_limit: int = 10,
) -> dict:
    kpi_predicate = and_(
        _null_source_predicate(),
        _kpi_form_predicate(),
    )
    apqp_predicate = and_(
        _null_source_predicate(),
        _apqp_predicate(),
        ~_kpi_form_predicate(),
    )
    overlap_predicate = and_(
        _null_source_predicate(),
        _kpi_form_predicate(),
        _apqp_predicate(),
    )

    kpi_count = _count_rows(db, kpi_predicate)
    apqp_count = _count_rows(db, apqp_predicate)
    overlap_count = _count_rows(db, overlap_predicate)
    total_would_touch = kpi_count + apqp_count

    result = {
        "dry_run": dry_run,
        "updated": False,
        "rules": {
            KPI_FORM_SOURCE_APPLICATION: {
                "count": kpi_count,
                "description": 'source_application IS NULL AND description contains "Link key: kpi-form|"',
                "sample": _sample_rows(db, kpi_predicate, sample_limit),
            },
            APQP_SOURCE_APPLICATION: {
                "count": apqp_count,
                "description": 'source_application IS NULL AND (code starts with "APQP-" OR inserted_by = "apqp-app")',
                "sample": _sample_rows(db, apqp_predicate, sample_limit),
            },
        },
        "overlap_count": overlap_count,
        "total_would_touch": total_would_touch,
        "outside_safe_rules_would_touch": 0,
        "safety": {
            "only_null_source_application": True,
            "never_overwrites_existing_source_application": True,
            "outside_safe_rules_touched": 0,
        },
    }

    if dry_run:
        return result

    kpi_updated = (
        db.query(Sujet)
        .filter(kpi_predicate)
        .update(
            {Sujet.source_application: KPI_FORM_SOURCE_APPLICATION},
            synchronize_session=False,
        )
    )
    apqp_updated = (
        db.query(Sujet)
        .filter(apqp_predicate)
        .update(
            {Sujet.source_application: APQP_SOURCE_APPLICATION},
            synchronize_session=False,
        )
    )
    db.commit()

    result["updated"] = True
    result["updated_counts"] = {
        KPI_FORM_SOURCE_APPLICATION: int(kpi_updated or 0),
        APQP_SOURCE_APPLICATION: int(apqp_updated or 0),
    }
    result["total_updated"] = int((kpi_updated or 0) + (apqp_updated or 0))

    return result
