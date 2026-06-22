import argparse
import asyncio
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config.database import SessionLocal
from app.services.action_priority_service import recalculate_all_priorities_service


async def run(dry_run: bool, include_deleted: bool):
    db = SessionLocal()

    try:
        return await recalculate_all_priorities_service(
            db,
            dry_run=dry_run,
            include_deleted=include_deleted,
        )
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Recalculate Action Plan priority fields.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Omit for dry-run mode.",
    )
    parser.add_argument(
        "--include-deleted",
        action="store_true",
        help="Also recalculate rows marked deleted. Omit to process only active rows.",
    )
    args = parser.parse_args()

    result = asyncio.run(run(dry_run=not args.apply, include_deleted=args.include_deleted))
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
