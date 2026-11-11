from crawler.tasks import (
    crawl_airfryer,
    crawl_fridge,
    crawl_tv,
    crawl_vacuum,
)


if __name__ == "__main__":
    task_queues = (
        (crawl_airfryer, "airfryer"),
        (crawl_fridge, "fridge"),
        (crawl_tv, "tv"),
        (crawl_vacuum, "vacuum"),
    )
    for task, queue in task_queues:
        result = task.apply_async(queue=queue)
        print(f"已派送 {task.name} 到 {queue} queue，Task ID: {result.id}")
