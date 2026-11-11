import getpass
from http.cookiejar import CookieJar
import json
from pathlib import Path
import re
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, HTTPRedirectHandler, Request, build_opener


ROOT = Path(__file__).resolve().parent
URL = "http://127.0.0.1:5006"
CREATE_ADMIN = """
import json
import sys
from redash import create_app
from redash.handlers.setup import create_org
from redash.models import Organization

details = json.load(sys.stdin)
app = create_app()
with app.app_context():
    if Organization.query.first() is not None:
        raise SystemExit("Redash is already initialized.")
    create_org("Home Appliance Comparison", details["name"], details["email"], details["password"])
"""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.opener = build_opener(HTTPCookieProcessor(CookieJar()), NoRedirect())

    def request(self, method, path, body=None, form=False):
        headers = {}
        data = None
        if body is not None:
            data = urlencode(body).encode() if form else json.dumps(body).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded" if form else "application/json"
        request = Request(self.base_url + path, data=data, method=method, headers=headers)
        try:
            response = self.opener.open(request, timeout=15)
        except HTTPError as error:
            if error.code not in (301, 302, 303):
                raise RuntimeError(f"Redash {method} {path} failed (HTTP {error.code})") from error
            response = error
        with response:
            data = response.read()
            content_type = response.headers.get("Content-Type", "")
            return response.status, json.loads(data) if "application/json" in content_type and data else None


def run_query(client, query_id):
    _, result = client.request("POST", f"/api/queries/{query_id}/results", {"max_age": 0})
    if "query_result" in result:
        return result["query_result"]["data"]["rows"]
    job_id = result["job"]["id"]
    for _ in range(60):
        _, response = client.request("GET", f"/api/jobs/{job_id}")
        job = response["job"]
        if job["status"] == 4:
            raise RuntimeError(f"Dropdown query failed: {job.get('error', 'unknown error')}")
        if job["status"] == 3:
            _, result = client.request("GET", f"/api/query_results/{job['query_result_id']}")
            return result["query_result"]["data"]["rows"]
        time.sleep(1)
    raise RuntimeError("Dropdown query timed out.")


def create_dashboard(client, db_user, db_password, database):
    status, dashboards = client.request("GET", "/api/dashboards?page_size=100")
    if status != 200 or not isinstance(dashboards, dict):
        raise RuntimeError("Cannot read dashboards; verify the Redash admin login.")
    if any(item["name"] == "價格分析" for item in dashboards["results"]):
        raise RuntimeError("A 價格分析 dashboard already exists; refusing to overwrite it.")

    _, existing = client.request("GET", "/api/data_sources")
    if existing:
        raise RuntimeError("Existing data sources found; refusing to modify an existing Redash installation.")
    _, source = client.request("POST", "/api/data_sources", {
        "name": "家電價格資料庫", "type": "mysql",
        "options": {"host": "mysql", "port": 3306, "user": db_user, "passwd": db_password, "db": database},
    })
    source_id = source["id"]
    queries = {}
    defaults = {}
    names = [
        ("categories", "商品種類"),
        ("dates", "日期"),
        ("price_analysis", "價格分析"),
        ("product_ranking", "商品排名"),
        ("high_rated_products", "高評價商品"),
    ]
    for key, name in names:
        parameters = []
        if key in ("price_analysis", "product_ranking"):
            for title, ref in (("商品種類", "categories"), ("日期", "dates")):
                parameters.append({
                    "title": title, "name": title, "type": "query", "global": False,
                    "locals": [], "queryId": queries[ref]["id"], "value": defaults[ref],
                })
        definition = {
            "name": name, "query": (ROOT / "queries" / f"{key}.sql").read_text(encoding="utf-8"),
            "data_source_id": source_id,
            "options": {"apply_auto_limit": key not in ("categories", "dates"), "parameters": parameters},
        }
        if key in ("categories", "dates"):
            definition["schedule"] = {"interval": 86400, "time": "04:30", "day_of_week": None, "until": None}
        _, query = client.request("POST", "/api/queries", definition)
        client.request("POST", f"/api/queries/{query['id']}", {"is_draft": False})
        queries[key] = query
        if key in ("categories", "dates"):
            rows = run_query(client, query["id"])
            column = "category" if key == "categories" else "scrape_date"
            candidates = [row[column] for row in rows if row.get(column)]
            defaults[key] = (max(candidates) if key == "dates" else sorted(candidates)[0]) if candidates else None

    chart_options = {
        "globalSeriesType": "column", "sortX": True,
        "legend": {"enabled": True, "placement": "auto", "traceorder": "normal"},
        "xAxis": {"type": "-", "labels": {"enabled": True}},
        "yAxis": [{"type": "linear"}, {"type": "linear", "opposite": True}],
        "alignYAxesAtZero": False, "error_y": {"type": "data", "visible": True},
        "series": {"stacking": None, "error_y": {"type": "data", "visible": True}},
        "seriesOptions": {}, "valuesOptions": {}, "columnMapping": {"排名": "x", "價格": "y"},
        "direction": {"type": "counterclockwise"}, "sizemode": "diameter", "coefficient": 1,
        "numberFormat": "0,0[.]00000", "percentFormat": "0[.]00%", "textFormat": "",
        "missingValuesAsZero": True, "showDataLabels": False,
        "dateTimeFormat": "DD/MM/YY HH:mm", "swappedAxes": False,
    }
    _, chart = client.request("POST", "/api/visualizations", {
        "query_id": queries["price_analysis"]["id"], "type": "CHART", "name": "Chart", "options": chart_options,
    })
    _, dashboard = client.request("POST", "/api/dashboards", {"name": "價格分析"})
    dashboard_id = dashboard["id"]
    mappings = {
        title: {"name": title, "type": "dashboard-level", "mapTo": title, "value": None, "title": ""}
        for title in ("商品種類", "日期")
    }
    widget_specs = [
        (chart["id"], mappings, 0, 10, 3, 9, 1, 5),
        (next(v["id"] for v in queries["product_ranking"]["visualizations"] if v["type"] == "TABLE"),
         mappings, 0, 0, 6, 10, 2, 1),
        (next(v["id"] for v in queries["high_rated_products"]["visualizations"] if v["type"] == "TABLE"),
         {}, 3, 10, 3, 9, 2, 1),
    ]
    for viz, mapping, col, row, width, height, min_width, min_height in widget_specs:
        client.request("POST", "/api/widgets", {
            "dashboard_id": dashboard_id, "visualization_id": viz, "text": "", "width": 1,
            "options": {"parameterMappings": mapping, "isHidden": False, "position": {
                "autoHeight": False, "sizeX": width, "sizeY": height, "minSizeX": min_width,
                "maxSizeX": 6, "minSizeY": min_height, "maxSizeY": 1000, "col": col, "row": row,
            }},
        })
    client.request("POST", f"/api/dashboards/{dashboard_id}", {"is_draft": False})
    print(f"Dashboard created: {client.base_url}/dashboards/{dashboard_id}")
    if any(value is None for value in defaults.values()):
        print("No matching MySQL data yet. Refresh the dropdown queries after importing or crawling data.")


def bootstrap(base_url, db_user, db_password, database, container_id=None):
    client = Client(base_url)
    for _ in range(90):
        try:
            status, _ = client.request("GET", "/setup")
            break
        except (URLError, TimeoutError):
            time.sleep(2)
    else:
        raise RuntimeError("Redash did not become ready; check its service logs.")
    if status in (301, 302, 303):
        print("Redash is already initialized; existing dashboards were not changed. Verify the dashboard in Redash.")
        return
    if status != 200:
        raise RuntimeError("Unexpected Redash setup response.")
    if container_id is None:
        result = subprocess.run(
            ["docker", "ps", "--filter", "label=com.docker.swarm.service.name=home_appliance_redash_redash",
             "--format", "{{.ID}}"], check=True, capture_output=True, text=True,
        )
        containers = result.stdout.splitlines()
        if len(containers) != 1:
            raise RuntimeError(
                "The Redash web container must run on this node for first-time initialization. "
                "Set redash=true on the manager node or initialize on the Redash node."
            )
        container_id = containers[0]
    print("New Redash installation: enter an administrator account.")
    name = input("Admin name: ").strip()
    email = input("Admin email: ").strip()
    password = getpass.getpass("Admin password (at least 6 characters): ")
    if not name or not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) or len(password) < 6:
        raise ValueError("Admin name, valid email and a password of at least 6 characters are required.")
    reader = input(f"MySQL user (Enter for {db_user}): ").strip()
    if reader:
        db_user = reader
        db_password = getpass.getpass("MySQL user password: ")
        if not db_password:
            raise ValueError("MySQL user password must not be empty.")
    subprocess.run(
        ["docker", "exec", "-i", container_id, "python", "-c", CREATE_ADMIN],
        input=json.dumps({"name": name, "email": email, "password": password}),
        text=True, check=True, stdout=subprocess.DEVNULL,
    )
    status, _ = client.request("POST", "/login", {"email": email, "password": password}, form=True)
    if status not in (301, 302, 303):
        raise RuntimeError("The administrator was created but login failed; inspect Redash before retrying.")
    try:
        create_dashboard(client, db_user, db_password, database)
    except (OSError, RuntimeError, ValueError, KeyError) as error:
        raise RuntimeError(
            "The admin account was created but dashboard setup did not finish. "
            f"Existing data was not removed; inspect Redash and retry manually. Cause: {error}"
        ) from error
