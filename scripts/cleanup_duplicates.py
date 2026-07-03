import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.database import SessionLocal
from app.config.organisation_database import OrganisationSessionLocal
from app.services.action_duplicate_service import (
    get_duplicate_action_groups_service,
    resolve_duplicate_actions_service,
)
from app.services.sujet_duplicate_service import (
    get_duplicate_sujet_groups_service,
    merge_duplicate_sujets_service,
)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Safely detect and clean duplicate Action Plan actions/sujets.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Omit this flag for dry-run mode.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Explicit dry-run flag for readability. Dry-run is the default.",
    )
    parser.add_argument(
        "--scope",
        choices=["all", "responsible", "requester", "team"],
        default="all",
    )
    parser.add_argument("--email", default=None)
    parser.add_argument("--keep", choices=["oldest", "newest"], default="oldest")
    parser.add_argument(
        "--include-closed",
        action="store_true",
        help="Allow closed/completed actions to be considered for duplicate cleanup.",
    )
    parser.add_argument(
        "--skip-actions",
        action="store_true",
        help="Skip duplicate action cleanup.",
    )
    parser.add_argument(
        "--skip-sujets",
        action="store_true",
        help="Skip duplicate sujet merge cleanup.",
    )
    return parser


def cleanup_actions(db, organisation_db, args, dry_run: bool):
    groups_response = get_duplicate_action_groups_service(
        db,
        email=args.email,
        scope=args.scope,
        include_deleted=False,
        include_closed=args.include_closed,
        organisation_db=organisation_db,
        limit=10000,
    )
    resolved_groups = []
    actor = SimpleNamespace(email="scripts/cleanup_duplicates.py")

    for group in groups_response.get("groups", []):
        result = resolve_duplicate_actions_service(
            db,
            action_ids=group.get("action_ids", []),
            dry_run=dry_run,
            strategy=f"soft_delete_duplicates_keep_{args.keep}",
            current_user=actor,
            keep=args.keep,
            include_closed=args.include_closed,
        )
        resolved_groups.append({
            "group_key": group.get("group_key"),
            "title": (group.get("actions") or [{}])[0].get("titre"),
            "reason": "same normalized title/root topic/parent/responsible/requester/due date",
            "resolution": result,
        })

    return {
        "detected_group_count": groups_response.get("duplicate_group_count", 0),
        "resolved_group_count": len(resolved_groups),
        "groups": resolved_groups,
    }


def cleanup_sujets(db, args, dry_run: bool):
    groups_response = get_duplicate_sujet_groups_service(db, limit=10000)
    merged_groups = []
    actor = SimpleNamespace(email="scripts/cleanup_duplicates.py")

    for group in groups_response.get("groups", []):
        sujets = group.get("sujets") or []
        if len(sujets) < 2:
            continue

        keep_sujet = sujets[0] if args.keep == "oldest" else sujets[-1]
        keep_sujet_id = keep_sujet["id"]
        merge_sujet_ids = [
            sujet["id"]
            for sujet in sujets
            if sujet["id"] != keep_sujet_id
        ]

        result = merge_duplicate_sujets_service(
            db,
            dry_run=dry_run,
            keep_sujet_id=keep_sujet_id,
            merge_sujet_ids=merge_sujet_ids,
            current_user=actor,
        )
        merged_groups.append({
            "group_key": group.get("group_key"),
            "reason": "same normalized sujet title and logical parent",
            "resolution": result,
        })

    return {
        "detected_group_count": groups_response.get("duplicate_group_count", 0),
        "merged_group_count": len(merged_groups),
        "groups": merged_groups,
    }


def main():
    parser = build_parser()
    args = parser.parse_args()
    dry_run = not args.apply

    db = SessionLocal()
    organisation_db = OrganisationSessionLocal() if OrganisationSessionLocal else None

    report = {
        "dry_run": dry_run,
        "apply": bool(args.apply),
        "scope": args.scope,
        "email": args.email,
        "keep": args.keep,
        "include_closed": args.include_closed,
        "hard_deleted_actions": [],
        "hard_deleted_sujets": [],
        "actions": None,
        "sujets": None,
    }

    try:
        if not args.skip_actions:
            report["actions"] = cleanup_actions(db, organisation_db, args, dry_run)

        if not args.skip_sujets:
            report["sujets"] = cleanup_sujets(db, args, dry_run)

        if dry_run:
            db.rollback()
    except Exception as exc:
        db.rollback()
        report["error"] = {
            "type": type(exc).__name__,
            "detail": str(exc),
        }
        print(json.dumps(report, default=str, indent=2, ensure_ascii=False))
        raise
    finally:
        if organisation_db is not None:
            organisation_db.close()
        db.close()

    print(json.dumps(report, default=str, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
