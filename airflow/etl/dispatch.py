import os
from urllib.parse import quote

from celery import Celery


def dispatch_crawl(category: str) -> None:
    username = quote(os.environ["WORKER_ACCOUNT"], safe="")
    password = quote(os.environ["WORKER_PASSWORD"], safe="")
    broker = f"amqp://{username}:{password}@appliance_rabbitmq:5672//"
    app = Celery("appliance_dispatch", broker=broker)
    result = app.send_task(f"crawler.tasks.crawl_{category}", queue=category)
    print(f"Dispatched {category} crawl; Celery task id={result.id}")
