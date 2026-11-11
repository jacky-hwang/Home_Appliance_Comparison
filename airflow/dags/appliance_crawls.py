from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.dispatch import dispatch_crawl

for category, hour in (
    ("airfryer", 9),
    ("fridge", 10),
    ("tv", 11),
    ("vacuum", 12),
):
    with DAG(
        dag_id=f"crawl_{category}",
        start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone("Asia/Taipei")),
        schedule=f"0 {hour} * * *",
        catchup=False,
        max_active_runs=1,
        tags=["appliance"],
    ):
        PythonOperator(
            task_id="dispatch",
            python_callable=dispatch_crawl,
            op_kwargs={"category": category},
            retries=0,
        )
