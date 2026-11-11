from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger

from crawler.tasks import (
    crawl_airfryer,
    crawl_fridge,
    crawl_tv,
    crawl_vacuum,
)

TIMEZONE = "Asia/Taipei"


def dispatch_task(task_name: str, queue: str) -> None:
    tasks = {
        "airfryer": crawl_airfryer,
        "fridge": crawl_fridge,
        "tv": crawl_tv,
        "vacuum": crawl_vacuum,
    }
    task = tasks[queue]
    result = task.apply_async(queue=queue)
    logger.info("Dispatched {} to {} queue; task id={}", task_name, queue, result.id)


def create_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    daily_jobs = (
        ("crawl_airfryer", "airfryer", 9),
        ("crawl_fridge", "fridge", 10),
        ("crawl_tv", "tv", 11),
        ("crawl_vacuum", "vacuum", 12),
    )

    for task_name, queue, hour in daily_jobs:
        scheduler.add_job(
            dispatch_task,
            trigger="cron",
            id=f"daily-{queue}",
            args=(task_name, queue),
            hour=hour,
            minute=0,
            coalesce=True,
            max_instances=1,
            replace_existing=True,
        )
        logger.info("Scheduled {} daily at {:02d}:00 {}", task_name, hour, TIMEZONE)

    return scheduler


def main() -> None:
    scheduler = create_scheduler()
    logger.info("Starting daily crawler scheduler")
    scheduler.start()


if __name__ == "__main__":
    main()
