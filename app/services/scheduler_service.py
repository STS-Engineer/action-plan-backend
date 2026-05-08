import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from app.config.database import SessionLocal
from app.services.action_reminder_service import send_grouped_due_date_reminders_service


scheduler = BackgroundScheduler()


def run_daily_reminders_job():
    db = SessionLocal()

    try:
        asyncio.run(send_grouped_due_date_reminders_service(db))
    finally:
        db.close()


def start_scheduler():
    if scheduler.running:
        return

    # DISABLED FOR NOW.
    # Remove comment when ready for production.
    #
    # scheduler.add_job(
    #     run_daily_reminders_job,
    #     "cron",
    #     hour=8,
    #     minute=0,
    #     id="daily_action_plan_reminders",
    #     replace_existing=True,
    # )

    scheduler.start()


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()