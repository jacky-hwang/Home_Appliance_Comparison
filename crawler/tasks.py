# 從 worker.py 匯入 Celery app 實例
# 所有的 task 都要透過 app 來註冊, 才能被 Celery worker 識別
from crawler.worker import app
from crawler.pc_airfryer import run as run_airfryer
from crawler.pc_fridge import run as run_fridge
from crawler.pc_tv import run as run_tv
from crawler.pc_vacuum import run as run_vacuum


@app.task()
def crawl_airfryer():
    return run_airfryer()


@app.task()
def crawl_fridge():
    return run_fridge()


@app.task()
def crawl_tv():
    return run_tv()


@app.task()
def crawl_vacuum():
    return run_vacuum()
