import os
import asyncio
import logging

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
logger = logging.getLogger(__name__)


def run_async_job(async_func):
    asyncio.run(async_func())


async def daily_reminders_job():
    db = SessionLocal()

    try:
        logger.info("[SCHEDULER] Updating overdue actions...")
        overdue_result = await update_overdue_actions_service(db)
        logger.info("[SCHEDULER] Overdue update result=%s", overdue_result)

        logger.info("[SCHEDULER] Recalculating priorities...")
        priority_result = await recalculate_all_priorities_service(db)
        logger.info("[SCHEDULER] Priority recalculation result=%s", priority_result)

        logger.info("[SCHEDULER] Running daily grouped reminders...")
        result = await send_grouped_due_date_reminders_service(db)
        logger.info("[SCHEDULER] Daily reminders result=%s", result)

    except Exception:
        logger.exception("[SCHEDULER] Daily reminders failed.")

    finally:
        db.close()


async def weekly_reports_job():
    db = SessionLocal()

    try:
        logger.info("[SCHEDULER] Running weekly responsable reports...")
        responsable_result = await send_weekly_responsable_reports_service(db)
        logger.info("[SCHEDULER] Weekly responsable reports result=%s", responsable_result)

        logger.info("[SCHEDULER] Running weekly demandeur reports...")
        demandeur_result = await send_weekly_demandeur_reports_service(db)
        logger.info("[SCHEDULER] Weekly demandeur reports result=%s", demandeur_result)
    except Exception:
        logger.exception("[SCHEDULER] Weekly reports failed.")
    finally:
        db.close()


def _read_int_env(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        logger.warning("[SCHEDULER] Invalid %s=%s. Using default=%s.", name, value, default)
        return default


def _log_scheduler_config(
    enabled: bool,
    daily_hour: int,
    daily_minute: int,
    weekly_day: str,
    weekly_hour: int,
    weekly_minute: int,
    started: bool,
):
    jobs = [
        {
            "id": job.id,
            "next_run_time": str(getattr(job, "next_run_time", None)),
        }
        for job in scheduler.get_jobs()
    ]

    logger.info(
        "[SCHEDULER] Config enabled=%s daily_hour=%s daily_minute=%s "
        "weekly_day=%s weekly_hour=%s weekly_minute=%s jobs=%s started=%s",
        enabled,
        daily_hour,
        daily_minute,
        weekly_day,
        weekly_hour,
        weekly_minute,
        jobs,
        started,
    )


def start_scheduler():
    if scheduler.running:
        logger.info("[SCHEDULER] Already running. jobs=%s", [job.id for job in scheduler.get_jobs()])
        return

    enabled = os.getenv("SCHEDULER_ENABLED", "false").lower() == "true"
    daily_hour = _read_int_env("DAILY_REMINDER_HOUR", 8)
    daily_minute = _read_int_env("DAILY_REMINDER_MINUTE", 0)
    weekly_day = os.getenv("WEEKLY_REPORT_DAY", "mon")
    weekly_hour = _read_int_env("WEEKLY_REPORT_HOUR", 8)
    weekly_minute = _read_int_env("WEEKLY_REPORT_MINUTE", 30)

    if not enabled:
        logger.info("[SCHEDULER] Disabled. Set SCHEDULER_ENABLED=true to activate.")
        _log_scheduler_config(
            enabled,
            daily_hour,
            daily_minute,
            weekly_day,
            weekly_hour,
            weekly_minute,
            False,
        )
        return

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

    logger.info("[SCHEDULER] Started.")
    _log_scheduler_config(
        enabled,
        daily_hour,
        daily_minute,
        weekly_day,
        weekly_hour,
        weekly_minute,
        scheduler.running,
    )


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("[SCHEDULER] Stopped.")
