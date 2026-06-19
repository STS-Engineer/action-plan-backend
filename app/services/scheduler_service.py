import asyncio
import datetime
import logging
import os
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from app.config.database import SessionLocal
from app.services.action_overdue_service import update_overdue_actions_service
from app.services.action_priority_service import recalculate_all_priorities_service
from app.services.action_reminder_service import send_grouped_due_date_reminders_service
from app.services.weekly_report_service import (
    send_weekly_demandeur_reports_service,
    send_weekly_responsable_reports_service,
)


DEFAULT_SCHEDULER_TIMEZONE = "Africa/Tunis"
DAILY_REMINDER_JOB_ID = "daily_action_plan_reminders"
WEEKLY_REPORT_JOB_ID = "weekly_action_plan_reports"
DAILY_REMINDER_LOCK_KEY = 7291001001
WEEKLY_REPORT_LOCK_KEY = 7291001002

logger = logging.getLogger(__name__)
scheduler = BackgroundScheduler(timezone=ZoneInfo(DEFAULT_SCHEDULER_TIMEZONE))
_scheduler_last_error: dict | None = None
_scheduler_last_started_at: str | None = None


def run_async_job(async_func):
    asyncio.run(async_func())


def _try_acquire_job_lock(db, job_id: str, lock_key: int) -> bool:
    acquired = bool(
        db.execute(
            text("SELECT pg_try_advisory_lock(:lock_key)"),
            {"lock_key": lock_key},
        ).scalar()
    )

    if acquired:
        logger.info("[SCHEDULER] Advisory lock acquired job_id=%s lock_key=%s", job_id, lock_key)
    else:
        logger.warning("[SCHEDULER] Advisory lock not acquired; skipping job_id=%s lock_key=%s", job_id, lock_key)

    return acquired


def _release_job_lock(db, job_id: str, lock_key: int):
    try:
        db.rollback()
        released = bool(
            db.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": lock_key},
            ).scalar()
        )
        logger.info(
            "[SCHEDULER] Advisory lock released job_id=%s lock_key=%s released=%s",
            job_id,
            lock_key,
            released,
        )
    except Exception:
        logger.exception("[SCHEDULER] Failed to release advisory lock job_id=%s lock_key=%s", job_id, lock_key)


async def daily_reminders_job():
    lock_db = SessionLocal()
    job_db = SessionLocal()
    lock_acquired = False

    try:
        lock_acquired = _try_acquire_job_lock(lock_db, DAILY_REMINDER_JOB_ID, DAILY_REMINDER_LOCK_KEY)
        if not lock_acquired:
            return

        logger.info("[SCHEDULER] Executing job_id=%s", DAILY_REMINDER_JOB_ID)
        logger.info("[SCHEDULER] Updating overdue actions...")
        overdue_result = await update_overdue_actions_service(job_db)
        logger.info("[SCHEDULER] Overdue update result=%s", overdue_result)

        logger.info("[SCHEDULER] Recalculating priorities...")
        priority_result = await recalculate_all_priorities_service(job_db)
        logger.info("[SCHEDULER] Priority recalculation result=%s", priority_result)

        logger.info("[SCHEDULER] Running daily grouped reminders...")
        result = await send_grouped_due_date_reminders_service(job_db)
        logger.info("[SCHEDULER] Daily reminders result=%s", result)

    except Exception:
        logger.exception("[SCHEDULER] Daily reminders failed.")

    finally:
        if lock_acquired:
            _release_job_lock(lock_db, DAILY_REMINDER_JOB_ID, DAILY_REMINDER_LOCK_KEY)
        job_db.close()
        lock_db.close()


async def weekly_reports_job():
    lock_db = SessionLocal()
    job_db = SessionLocal()
    lock_acquired = False

    try:
        lock_acquired = _try_acquire_job_lock(lock_db, WEEKLY_REPORT_JOB_ID, WEEKLY_REPORT_LOCK_KEY)
        if not lock_acquired:
            return

        logger.info("[SCHEDULER] Executing job_id=%s", WEEKLY_REPORT_JOB_ID)
        logger.info("[SCHEDULER] Recalculating priorities before weekly reports...")
        priority_result = await recalculate_all_priorities_service(job_db)
        logger.info("[SCHEDULER] Weekly priority recalculation result=%s", priority_result)

        logger.info("[SCHEDULER] Running weekly responsable reports...")
        responsable_result = await send_weekly_responsable_reports_service(job_db)
        logger.info("[SCHEDULER] Weekly responsable reports result=%s", responsable_result)

        logger.info("[SCHEDULER] Running weekly demandeur reports...")
        demandeur_result = await send_weekly_demandeur_reports_service(job_db)
        logger.info("[SCHEDULER] Weekly demandeur reports result=%s", demandeur_result)
    except Exception:
        logger.exception("[SCHEDULER] Weekly reports failed.")
    finally:
        if lock_acquired:
            _release_job_lock(lock_db, WEEKLY_REPORT_JOB_ID, WEEKLY_REPORT_LOCK_KEY)
        job_db.close()
        lock_db.close()


def _read_int_env(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        logger.warning("[SCHEDULER] Invalid %s=%s. Using default=%s.", name, value, default)
        return default


def _read_enabled_env() -> bool:
    return os.getenv("SCHEDULER_ENABLED", "false").strip().lower() == "true"


def _read_daily_reminders_enabled_env() -> bool:
    return os.getenv("DAILY_REMINDERS_ENABLED", "false").strip().lower() == "true"


def _read_timezone_name() -> str:
    return os.getenv("SCHEDULER_TIMEZONE", DEFAULT_SCHEDULER_TIMEZONE).strip() or DEFAULT_SCHEDULER_TIMEZONE


def _read_scheduler_config():
    return {
        "scheduler_enabled_env": os.getenv("SCHEDULER_ENABLED", "false"),
        "scheduler_enabled": _read_enabled_env(),
        "daily_reminders_enabled_env": os.getenv("DAILY_REMINDERS_ENABLED", "false"),
        "daily_reminders_enabled": _read_daily_reminders_enabled_env(),
        "timezone": _read_timezone_name(),
        "daily_reminder_hour": _read_int_env("DAILY_REMINDER_HOUR", 8),
        "daily_reminder_minute": _read_int_env("DAILY_REMINDER_MINUTE", 0),
        "weekly_report_day": os.getenv("WEEKLY_REPORT_DAY", "mon"),
        "weekly_report_hour": _read_int_env("WEEKLY_REPORT_HOUR", 8),
        "weekly_report_minute": _read_int_env("WEEKLY_REPORT_MINUTE", 30),
    }


def _load_timezone(timezone_name: str):
    return ZoneInfo(timezone_name)


def _serialize_next_run_time(next_run_time):
    if not next_run_time:
        return None

    return next_run_time.isoformat()


def _serialize_job(job):
    next_run_time = getattr(job, "next_run_time", None)

    return {
        "id": job.id,
        "name": job.name,
        "trigger": str(job.trigger),
        "next_run_time": _serialize_next_run_time(next_run_time),
    }


def get_scheduler_status():
    config = _read_scheduler_config()
    jobs = [_serialize_job(job) for job in scheduler.get_jobs()]
    registered_job_ids = {job["id"] for job in jobs}

    return {
        "scheduler_enabled_env": config["scheduler_enabled_env"],
        "scheduler_enabled": config["scheduler_enabled"],
        "daily_reminders_enabled_env": config["daily_reminders_enabled_env"],
        "daily_reminders_enabled": config["daily_reminders_enabled"],
        "scheduler_running": scheduler.running,
        "timezone": config["timezone"],
        "scheduler_timezone": str(getattr(scheduler, "timezone", "")),
        "daily_reminder_schedule": {
            "hour": config["daily_reminder_hour"],
            "minute": config["daily_reminder_minute"],
        },
        "weekly_report_schedule": {
            "day": config["weekly_report_day"],
            "hour": config["weekly_report_hour"],
            "minute": config["weekly_report_minute"],
        },
        "registered_jobs": jobs,
        "daily_reminder_job_id": DAILY_REMINDER_JOB_ID,
        "weekly_report_job_id": WEEKLY_REPORT_JOB_ID,
        "daily_job_registered": DAILY_REMINDER_JOB_ID in registered_job_ids,
        "weekly_job_registered": WEEKLY_REPORT_JOB_ID in registered_job_ids,
        "last_error": _scheduler_last_error,
        "last_started_at": _scheduler_last_started_at,
    }


def _log_scheduler_status(started: bool):
    status = get_scheduler_status()
    logger.info(
        "[SCHEDULER] Config SCHEDULER_ENABLED=%s timezone=%s "
        "DAILY_REMINDERS_ENABLED=%s daily=%02d:%02d weekly=%s %02d:%02d "
        "daily_job_registered=%s weekly_job_registered=%s jobs=%s started=%s last_error=%s",
        status["scheduler_enabled_env"],
        status["timezone"],
        status["daily_reminders_enabled_env"],
        status["daily_reminder_schedule"]["hour"],
        status["daily_reminder_schedule"]["minute"],
        status["weekly_report_schedule"]["day"],
        status["weekly_report_schedule"]["hour"],
        status["weekly_report_schedule"]["minute"],
        status["daily_job_registered"],
        status["weekly_job_registered"],
        status["registered_jobs"],
        started,
        status["last_error"],
    )


def _shutdown_scheduler_safely():
    global scheduler

    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("[SCHEDULER] Stopped.")
    except Exception:
        logger.exception("[SCHEDULER] Failed while stopping scheduler.")


def _configure_scheduler(config, timezone):
    global scheduler

    scheduler = BackgroundScheduler(timezone=timezone)

    if config["daily_reminders_enabled"]:
        scheduler.add_job(
            lambda: run_async_job(daily_reminders_job),
            CronTrigger(
                hour=config["daily_reminder_hour"],
                minute=config["daily_reminder_minute"],
                timezone=timezone,
            ),
            id=DAILY_REMINDER_JOB_ID,
            replace_existing=True,
        )
        logger.info("[SCHEDULER] Daily reminders enabled.")
    else:
        logger.info("[SCHEDULER] Daily reminders disabled by configuration.")

    scheduler.add_job(
        lambda: run_async_job(weekly_reports_job),
        CronTrigger(
            day_of_week=config["weekly_report_day"],
            hour=config["weekly_report_hour"],
            minute=config["weekly_report_minute"],
            timezone=timezone,
        ),
        id=WEEKLY_REPORT_JOB_ID,
        replace_existing=True,
    )
    logger.info("[SCHEDULER] Weekly reports enabled.")


def start_scheduler():
    global _scheduler_last_error, _scheduler_last_started_at, scheduler

    if scheduler.running:
        logger.info("[SCHEDULER] Already running. jobs=%s", [job.id for job in scheduler.get_jobs()])
        _log_scheduler_status(True)
        return get_scheduler_status()

    config = _read_scheduler_config()

    try:
        timezone = _load_timezone(config["timezone"])

        if not config["scheduler_enabled"]:
            logger.info("[SCHEDULER] Disabled. Set SCHEDULER_ENABLED=true to activate.")
            scheduler = BackgroundScheduler(timezone=timezone)
            _scheduler_last_error = None
            _log_scheduler_status(False)
            return get_scheduler_status()

        _configure_scheduler(config, timezone)
        scheduler.start()
        _scheduler_last_error = None
        _scheduler_last_started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info("[SCHEDULER] Started.")
        _log_scheduler_status(scheduler.running)
        return get_scheduler_status()
    except Exception as exc:
        _scheduler_last_error = {
            "error_type": type(exc).__name__,
            "error_detail": str(exc),
        }
        logger.exception("[SCHEDULER] Failed to start scheduler.")
        _log_scheduler_status(False)
        return get_scheduler_status()


def reload_scheduler():
    _shutdown_scheduler_safely()
    return start_scheduler()


def stop_scheduler():
    _shutdown_scheduler_safely()
