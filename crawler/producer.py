from crawler.tasks import (
    crawl_airfryer,
    crawl_fridge,
    crawl_tv,
    crawl_vacuum,
)

if __name__ == "__main__":
    tasks = (crawl_airfryer, crawl_fridge, crawl_tv, crawl_vacuum)
    for task in tasks:
        result = task.delay()
        print(f"已派送 {task.name} 到 celery queue，Task ID: {result.id}")
