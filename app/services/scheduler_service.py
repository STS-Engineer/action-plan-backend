import os
import asyncio

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config.database import SessionLocal
from app.services.action_reminder_service import send_grouped_due_date_reminders_service
from app.services.action_priority_service import recalculate_all_priorities_service
from app.services.weekly_report_service import (
    send_weekly_responsable_reports_service,
    send_weekly_demandeur_reports_service,
)
from app.services.action_overdue_service import update_overdue_actions_service

scheduler = BackgroundScheduler(timezone="Africa/Tunis")


def run_async_job(async_func):
    asyncio.run(async_func())


async def daily_reminders_job():
    db = SessionLocal()

    try:
        print("[SCHEDULER] Updating overdue actions...")
        overdue_result = await update_overdue_actions_service(db)
        print("[SCHEDULER] Overdue update result:", overdue_result)

        print("[SCHEDULER] Recalculating priorities...")
        priority_result = await recalculate_all_priorities_service(db)
        print("[SCHEDULER] Priority recalculation result:", priority_result)

        print("[SCHEDULER] Running daily grouped reminders...")
        result = await send_grouped_due_date_reminders_service(db)
        print("[SCHEDULER] Daily reminders result:", result)

    except Exception as e:
        print("[SCHEDULER] Daily reminders failed:", str(e))

    finally:
        db.close()


async def weekly_reports_job():
    db = SessionLocal()

    try:
        print("[SCHEDULER] Running weekly responsable reports...")
        responsable_result = await send_weekly_responsable_reports_service(db)
        print("[SCHEDULER] Weekly responsable reports result:", responsable_result)

        print("[SCHEDULER] Running weekly demandeur reports...")
        demandeur_result = await send_weekly_demandeur_reports_service(db)
        print("[SCHEDULER] Weekly demandeur reports result:", demandeur_result)
    except Exception as e:
        print("[SCHEDULER] Weekly reports failed:", str(e))
    finally:
        db.close()


def start_scheduler():
    if scheduler.running:
        return

    enabled = os.getenv("SCHEDULER_ENABLED", "false").lower() == "true"

    if not enabled:
        print("[SCHEDULER] Disabled. Set SCHEDULER_ENABLED=true to activate.")
        return

    daily_hour = int(os.getenv("DAILY_REMINDER_HOUR", "8"))
    daily_minute = int(os.getenv("DAILY_REMINDER_MINUTE", "0"))

    weekly_day = os.getenv("WEEKLY_REPORT_DAY", "mon")
    weekly_hour = int(os.getenv("WEEKLY_REPORT_HOUR", "8"))
    weekly_minute = int(os.getenv("WEEKLY_REPORT_MINUTE", "30"))

    scheduler.add_job(
        lambda: run_async_job(daily_reminders_job),
        CronTrigger(hour=daily_hour, minute=daily_minute),
        id="daily_action_plan_reminders",
        replace_existing=True,
    )

    scheduler.add_job(
        lambda: run_async_job(weekly_reports_job),
        CronTrigger(day_of_week=weekly_day, hour=weekly_hour, minute=weekly_minute),
        id="weekly_action_plan_reports",
        replace_existing=True,
    )

    scheduler.start()

    print("[SCHEDULER] Started.")
    print(f"[SCHEDULER] Daily reminders: {daily_hour:02d}:{daily_minute:02d}")
    print(f"[SCHEDULER] Weekly reports: {weekly_day} {weekly_hour:02d}:{weekly_minute:02d}")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        print("[SCHEDULER] Stopped.")
