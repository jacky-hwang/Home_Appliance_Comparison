# Home Appliance Comparison

以 PChome 家電排行榜為資料來源，定時擷取**氣炸鍋、冰箱、電視和掃地機器人**各前 20 名商品，將資料寫入 CSV 與 MySQL，並透過 FastAPI 查詢商品價格、歷史價格和價格變動。

專案也提供獨立的 Airflow 排程及 Redash 儀表板 Swarm stacks。

**主要技術：** Python · Playwright · Celery · RabbitMQ · MySQL · FastAPI · Airflow · Redash · Docker Swarm

## 目錄

- [技術架構](#技術架構)
- [專案結構](#專案結構)
- [環境設定與快速啟動](#環境設定與快速啟動)
  - [需求](#需求)
  - [初次設定](#初次設定)
  - [啟動服務（Compose）](#啟動服務compose)
- [常用指令](#常用指令)
  - [管理介面網址](#管理介面網址)
- [爬蟲、資料庫與排程](#爬蟲資料庫與排程)
  - [分類 queue 與每日排程](#分類-queue-與每日排程)
- [API 使用與開發](#api-使用與開發)
- [Redash Swarm 儀表板](#redash-swarm-儀表板)
- [建置與部署 Docker image](#建置與部署-docker-image)
- [Docker Swarm 本機演練與 GCP 部署準備](#docker-swarm-本機演練與-gcp-部署準備)
  - [Airflow Swarm 排程](#airflow-swarm-排程)

## 技術架構

| 元件 | 用途 |
|---|---|
| Crawler | 使用 Celery、RabbitMQ 和 Playwright 擷取四種家電排行榜 |
| MySQL | 保存爬蟲結果；同一天、分類和排名只保留一筆，重爬時更新原紀錄 |
| FastAPI | 查詢分類、商品、價格比較、歷史價格和價格變動 |
| Airflow | Swarm 每日排程、手動派送及查看派送記錄 |
| Redash | 連接 MySQL，建立圖表和儀表板 |

**資料處理流程**

```text
Airflow 排程 → RabbitMQ → 爬蟲 worker → MySQL → FastAPI / Redash
                              └→ CSV
```

> **排程說明：** Airflow 負責派送，爬蟲 worker 負責抓取及寫入資料；Airflow 的派送成功不代表爬蟲已完成。

## 專案結構

```text
Home_Appliance_Comparison/
├── airflow/                         # Airflow 設定、每日派送 DAG 與 ETL 任務
├── api/                             # FastAPI 原始碼與獨立 Python 專案設定
├── crawler/                         # 爬蟲、worker、producer、排程器與 CSV 匯入
├── data/                            # 爬蟲輸出的 CSV，可供公開與匯入
├── database/                        # MySQL 初始化、資料表 migration 和備份腳本
├── redash/                          # 價格分析儀表板初始化及 SQL 查詢
├── services/                        # Redash Compose 設定及環境變數範本
├── docker-compose.yml               # 主服務：MySQL、RabbitMQ、Crawler、API
├── docker-compose.swarm.yml         # 應用程式 Swarm stack
├── docker-compose.airflow.swarm.yml # 獨立 Airflow Swarm stack
├── portainer.yml                    # Portainer Swarm stack
├── Dockerfile                       # Crawler image
├── Dockerfile.api                   # API image
├── Dockerfile.airflow               # Airflow image 和 DAG
├── genenv.py                        # 從 local.ini 產生環境設定
├── manage_env.py                    # 環境設定管理與 Swarm 部署
└── local.ini.example                # DEV / DOCKER 環境設定範本
```

完整操作說明集中在本文件；[api/README.md](./api/README.md) 保留中文快速導引。

## 環境設定與快速啟動

### 需求

- Docker Engine 與 Docker Compose v2
- Python 3.11 以上版本及 [uv](https://docs.astral.sh/uv/)（用於本機執行、產生 `.env`）
- 本機直接執行爬蟲時需要 Playwright Chromium；Docker image 已包含所需瀏覽器

### 初次設定

**1. 建立設定檔**

首次在專案根目錄執行；若 `local.ini` 已存在，請勿覆蓋：

```bash
cp local.ini.example local.ini
```

`local.ini` 分成 `[DEV]` 和 `[DOCKER]` 兩組設定：本機程式使用 `DEV`，Compose 容器使用 `DOCKER`。範例中的帳密只供本機開發，部署到其他主機前請換成安全的帳密。`local.ini` 和產生的 `.env` 含有憑證，不要提交到版本控制。

**2. 產生環境變數並準備資料目錄**

產生 Docker 環境使用的 `.env`，並準備 CSV 輸出目錄。此指令會更新 `local.ini` 對應的設定，但保留其他設定（包括 Airflow 和 Redash 金鑰）；手動修改過的相同項目仍會被 `local.ini` 的值取代：

```bash
ENV=DOCKER uv run python genenv.py
mkdir -p data
```

**舊版遷移（僅適用既有環境）**

Airflow、Redash 也使用根目錄這一份 `.env`。只有從舊版遷移、且仍同時保有 `.env.airflow` 與 `services/.env` 兩個檔案時，才執行以下指令合併；原檔會保留。若檔案已刪除或已完成合併，無須執行。若同名設定的值不同，指令會停止並指出衝突，請先確認後再處理：

```bash
python3 manage_env.py migrate
```

> **安全提醒：** `.env` 包含所有服務的機密，請限制檔案權限、另行安全備份，勿提交或分享。服務僅接收各自需要的設定；Swarm 管理員仍可查看服務環境變數。確認遷移與服務運作後，請妥善保管舊檔，不要將舊檔當作持續編輯的設定。

本機直接使用 `uv run --env-file .env` 執行程式時，該程式會讀入整份 `.env`，請只在受信任的環境執行。

### 啟動服務（Compose）

此方式適合本機開發，與下方 Swarm 部署是不同模式。

> **注意：** 不要同時啟動兩套服務，否則可能發生連接埠衝突或重複派送。

啟動預設服務（MySQL、RabbitMQ、API、一般 worker 等；不會啟動每日排程）：

```bash
docker compose up --build -d
docker compose ps
```

API 健康檢查：<http://localhost:8889/health>；Swagger 文件：<http://localhost:8889/docs>。

如需 Compose 每日排程及四個分類專用 worker，依照[排程章節](#爬蟲資料庫與排程)啟動 `multi-queue` profile。要使用 Swarm 的 Airflow 排程，請改依 [Swarm 部署步驟](#docker-swarm-本機演練與-gcp-部署準備)部署，不要同時啟動 Compose scheduler。

## 常用指令

以下指令皆在專案根目錄執行：

### 管理介面網址

以下網址供部署在本機時使用；先確認對應服務已啟動。Compose 與 Swarm 是不同部署方式，請依目前使用的方式查看。

| 服務 | 網址／主機連接埠 | 用途 |
|---|---|---|
| Portainer（Swarm） | <http://localhost:9000> | 管理 stacks、服務及節點 |
| Airflow（Swarm） | <http://localhost:8083> | 查看 DAG、排程及派送日誌 |
| Airflow Flower（Swarm） | <http://localhost:5556> | 監控 Airflow worker |
| 爬蟲 Flower | <http://localhost:5557> | 監控爬蟲 worker 與任務 |
| RabbitMQ 管理頁 | <http://localhost:15674> | 查看 queues 與訊息 |
| phpMyAdmin | <http://localhost:8082> | 管理 MySQL 資料 |
| Redash（Swarm） | <http://localhost:5006> | 查看查詢、圖表及儀表板 |
| API 文件 | <http://localhost:8889/docs> | 使用 Swagger UI 測試 API |
| API 健康檢查 | <http://localhost:8889/health> | 確認 API 和資料庫連線 |

**登入資訊**由根目錄 `.env` 或初始化時建立的帳號決定：

- Airflow Flower：`FLOWER_BASIC_AUTH`。
- Swarm 爬蟲 Flower：`WORKER_ACCOUNT`／`WORKER_PASSWORD`。
- RabbitMQ：Swarm 使用 `RABBITMQ_DEFAULT_USER`／`RABBITMQ_DEFAULT_PASS`；Compose 使用 `WORKER_ACCOUNT`／`WORKER_PASSWORD`。
- Airflow、Redash、Portainer：使用各自建立的帳號。

Compose 的爬蟲 Flower 未設定登入驗證，僅供受信任的本機環境使用。

本機 Compose 額外提供 MySQL `127.0.0.1:3308` 和 RabbitMQ AMQP `127.0.0.1:5674`；Swarm 設定沒有對主機發布這兩個資料庫／訊息連接埠，請透過對應服務或管理介面連線。

> **安全提醒：** Swarm 發布的管理介面不保證只限本機；部署到共享主機或雲端時，請使用強密碼、限制防火牆來源並搭配 HTTPS，不要直接公開管理介面。

### Swarm：查看服務與節點

以下指令在 Swarm manager 上執行，適用於已部署的 stacks。首次部署與設定請參考 [Docker Swarm 部署](#docker-swarm-本機演練與-gcp-部署準備)。

```bash
# 查看節點、所有 stacks 與服務
docker node ls
docker stack ls
docker service ls

# 查看各 stack 的服務及執行狀態
docker stack services appliance
docker stack services home_appliance_airflow
docker stack services home_appliance_redash
docker stack services portainer
docker stack ps appliance

# 查看目前節點的 labels、網路和 volumes
docker node inspect self --format '{{json .Spec.Labels}}'
docker network ls
docker volume ls

# 查看容器狀態和即時資源用量
docker ps
docker stats --no-stream
```

`1/1` 表示一個預期副本正在執行，不一定代表應用程式已準備好；若顯示 `0/1`，可用 `docker stack ps` 和服務日誌檢查原因。`docker ps` 和 `docker stats` 只顯示執行指令的節點上的容器。查看 volume 不等於備份資料，請勿直接刪除資料庫使用中的 volume。

### Swarm：查看日誌與排程

```bash
# API、RabbitMQ 和分類爬蟲 worker
docker service logs --tail 100 appliance_api
docker service logs --tail 100 appliance_rabbitmq
docker service logs --tail 100 appliance_worker_airfryer
docker service logs --tail 100 appliance_worker_fridge
docker service logs --tail 100 appliance_worker_tv
docker service logs --tail 100 appliance_worker_vacuum

# Airflow 和 Redash
docker service logs --tail 100 home_appliance_airflow_scheduler
docker service logs --tail 100 home_appliance_airflow_worker
docker service logs --tail 100 home_appliance_redash_redash

# 確認 API 與 Airflow 健康狀態
curl http://localhost:8889/health
curl http://localhost:8083/health
```

在任一日誌指令加上 `-f` 可持續查看，按 `Ctrl+C` 結束。查看排程或手動執行時，開啟 [Airflow](http://localhost:8083)，選擇 DAG 後查看 Graph 和 Logs；手動觸發會真的派送爬蟲，可能與定時任務重複。Airflow 顯示成功只代表任務已派送，爬蟲是否完成請查看爬蟲 Flower 或 worker 日誌。

部署、更新和停止各 stack 的指令請分別參考 [應用程式](#部署應用程式-stack單節點測試雙節點部署)、[Airflow](#airflow-swarm-排程)、[Redash](#redash-swarm-儀表板) 和 [Portainer](#初始化-swarm-與部署-portainer) 章節。

### 查詢今天的爬蟲資料

在 [phpMyAdmin](http://localhost:8082) 選擇 `tibame` 資料庫，於 SQL 頁面執行（`CURDATE()` 以 MySQL 的時區為準）：

```sql
SELECT category, COUNT(*) AS items
FROM crawl_results
WHERE scrape_date = CURDATE()
GROUP BY category;
```

這會顯示各分類當天已寫入的筆數；資料不完整時，可再查看爬蟲 worker 日誌。

### Compose：服務操作

```bash
# 檢查 Compose 設定與服務狀態
docker compose config -q
docker compose ps

# 查看所有服務或指定服務的日誌
docker compose logs -f
docker compose logs -f api
docker compose logs -f worker
docker compose logs -f worker_airfryer

# 重啟指定服務
docker compose restart api

# 停止服務，但保留資料
docker compose stop
docker compose down
```

> **注意：** `docker compose down -v` 會刪除 Compose 的資料 volume，包含 MySQL 資料；只有確定要清除資料時才使用。

### Compose：手動派送爬蟲任務

以下指令會實際執行爬蟲及寫入資料。一般模式將四種任務送進預設 `celery` queue，需先啟動一般 `worker`：

```bash
docker compose --profile tools run --rm producer
```

分類 queue 模式需要先啟動四個專用 worker，再派送任務：

```bash
docker compose --profile multi-queue up -d rabbitmq flower worker_airfryer worker_fridge worker_tv worker_vacuum
docker compose --profile tools run --rm producer_multi_queue
```

### Compose：RabbitMQ、MySQL 備份與資料匯入

查看 RabbitMQ 中等待處理的訊息：

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready
```

備份 Compose 的 MySQL 爬蟲資料庫：

```bash
bash database/backup_mysql.sh
```

備份檔放在 `database/backups/`，不應提交到版本控制。此腳本使用 `docker compose exec`，不適用於 Swarm 服務，且不包含 Airflow metadata 或 Redash 資料；Swarm 部署需另行備份。將 `data/` 中既有 CSV 匯入 MySQL，先執行不寫入資料庫的 dry-run：

```bash
docker compose --profile multi-queue run --rm --build worker_airfryer \
  python -m crawler.import_csv --input-dir /data --dry-run
```

確認匯入筆數後，移除 `--dry-run` 再執行；正式匯入會寫入或更新 MySQL 資料。Compose 容器內的 `/data` 對應專案根目錄的 `data/`。

在 phpMyAdmin 或 MySQL client 選擇 `tibame` 資料庫後，可查看最新爬取資料：

```sql
SELECT scrape_date, category, rank_position, product_name, special_price, product_id
FROM crawl_results
ORDER BY scrape_date DESC, category, rank_position
LIMIT 100;
```

## 爬蟲、資料庫與排程

### 爬蟲與 CSV

爬蟲嘗試擷取以下分類各前 20 名，結果寫入 MySQL 的 `crawl_results` 和 CSV；實際筆數取決於來源及執行結果：

| 分類 | CSV 檔案 |
|---|---|
| 氣炸鍋 | `pc_airfryer_top20.csv` |
| 冰箱 | `pc_fridge_top20.csv` |
| 電視 | `pc_tv_top20.csv` |
| 掃地機器人 | `pc_vacuum_top20.csv` |

Compose 的 MySQL 使用 `mysql_data` volume，CSV 寫入專案根目錄的 `data/`。Swarm 的 MySQL 使用外部 volume `home_appliance_comparison_mysql_data`，CSV 則寫入 `appliance_crawler_data` volume，不會自動出現在專案的 `data/`。

Swarm 的 local volume 屬於各節點：worker 移到其他節點時，CSV 不會自動跟著搬移，也不保證所有節點共用同一份。重新建立容器通常會保留 volume；`docker compose down -v` 會刪除 Compose 的資料 volume。**Volume 不是備份**，重要資料應另存到其他位置並驗證還原。

資料庫初始化檔案位於 `database/`：`init.sql` 建立資料庫、`migrations/001_create_crawl_results.sql` 建立爬蟲資料表、`migrations/002_unique_date_category_rank.sql` 為既有資料表增加日期／分類／排名唯一索引。MySQL 初始化腳本只會在空白資料 volume 第一次啟動時執行；既有 volume 不會被重設，爬蟲也會檢查必要索引。

爬蟲和 API 的 PyMySQL 依賴使用 `rsa` extra，以支援 MySQL 的 `caching_sha2_password`／`sha256_password` 驗證。若 worker 日誌顯示缺少 `cryptography`，請建置新版爬蟲 image、更新 worker 使用的版本，並在多節點部署前推送 image；只重啟使用舊 image 的容器無法修復。API 也應重新建置並更新，以免遇到相同驗證錯誤。更新後失敗任務不會自動補跑，可確認資料後再透過 Airflow 手動觸發需要的分類。

phpMyAdmin 登入時使用 `local.ini` 的 `MYSQL_USER` / `MYSQL_PASSWORD`；此帳號可管理 `tibame` 資料庫中的表格。要建立其他資料庫則需使用 `root` 和 `MYSQL_ROOT_PASSWORD`。Redash 若供多人使用，請另建唯讀帳號。

### 分類 queue 與每日排程

本機 Compose 的 `multi-queue` profile 會啟動四個分類專用 worker 和 APScheduler；Swarm 則使用下方的 [Airflow stack](#airflow-swarm-排程)。兩者依台灣時間每天派送：

| 時間 | 分類 |
|---|---|
| 09:00 | 氣炸鍋 |
| 10:00 | 冰箱 |
| 11:00 | 電視 |
| 12:00 | 掃地機器人 |

啟動 RabbitMQ、Flower、專用 worker 和單一 scheduler：

```bash
docker compose --profile multi-queue up -d --build \
  rabbitmq flower worker_airfryer worker_fridge worker_tv worker_vacuum scheduler
```

不要同時啟動多個 scheduler（包括 Swarm Airflow），避免重複派送。Compose 的 APScheduler 只會在排程時間派送任務，不會於啟動時立刻爬取；Airflow 的啟動行為不同，請參考 [Airflow 章節](#airflow-swarm-排程)。查看 scheduler 或 worker 日誌：

```bash
docker compose logs -f scheduler
docker compose logs -f worker_airfryer
```

停止每日排程但保留其他服務：

```bash
docker compose --profile multi-queue stop scheduler
```

### 本機直接執行爬蟲

安裝專案依賴及 Chromium：

```bash
uv sync
uv run python -m playwright install chromium
```

若 Linux 缺少 Chromium 系統函式庫，可執行：

```bash
uv run python -m playwright install --with-deps chromium
```

先啟動 Compose 的 RabbitMQ、MySQL，再產生供主機端程式使用的 DEV 設定。此指令會更新 `.env` 中 `local.ini` 對應的項目；重新操作 Compose 前，請使用 `ENV=DOCKER uv run python genenv.py` 還原 Docker 設定：

```bash
docker compose up -d rabbitmq mysql
ENV=DEV uv run python genenv.py
```

分別在兩個終端機先啟動 worker，再執行 producer。producer 會實際派送四個爬蟲任務：

```bash
uv run --env-file .env celery -A crawler.worker worker --loglevel=info
uv run --env-file .env python crawler/producer.py
```

Docker 網路內的容器以服務名稱連線，例如 RabbitMQ 使用 `rabbitmq:5672`、MySQL 使用 `mysql:3306`；主機端程式則使用 `127.0.0.1:5674` 和 `127.0.0.1:3308`。
Compose 會自動建立名為 `home_appliance_comparison_network` 的 bridge network；需要檢查網路或容器 DNS 時可執行：

```bash
docker network inspect home_appliance_comparison_network
docker compose exec worker getent hosts rabbitmq
```

切換執行環境時，先停止另一邊的 worker，避免本機程式和 Docker worker 同時消費任務。例如從 Docker 切回本機：

```bash
docker compose --profile multi-queue stop worker worker_airfryer worker_fridge worker_tv worker_vacuum
ENV=DEV uv run python genenv.py
```

## API 使用與開發

API 以唯讀方式查詢 crawler 寫入的 `tibame.crawl_results`。除 Swagger 外，也可直接呼叫以下端點：

| 方法 | 路徑 | 說明 |
|---|---|---|
| `GET` | `/health` | 檢查資料庫連線 |
| `GET` | `/categories` | 列出有資料的分類、最新爬取日期及追蹤商品數 |
| `GET` | `/products` | 查詢每個商品編號最新一筆資料；支援篩選、排序、分頁 |
| `GET` | `/products/compare` | 比較 2–20 個商品編號的最新價格 |
| `GET` | `/products/{product_id}/history` | 查詢指定商品歷史價格及變動 |
| `GET` | `/price-changes` | 依分類、日期及方向查詢價格變動 |

查詢範例：

```text
/products?category=冰箱&sort_by=price_asc
/products?keyword=Panasonic&min_price=10000&max_price=50000
/products/compare?product_ids=DPAC1T-A900AVR5Q&product_ids=DPAC1T-A900FLZS4
/products/DPAC1T-A900AVR5Q/history
/price-changes?category=冰箱&direction=down
```

`/products` 的 `sort_by` 支援 `price_asc`、`price_desc`、`rank`、`newest`；每頁最多 100 筆。分類名稱為 `氣炸鍋`、`冰箱`、`電視`、`掃地機器人`。商品價格取自 PChome 的 `special_price`；歷史價格依同一個 PChome 商品編號比較，不是跨購物平台比價。

### 本機開發啟動 API

先在專案根目錄啟動 Compose MySQL，再產生 DEV 設定；此指令會更新 `.env` 中 `local.ini` 對應的項目。回到 Compose 操作前，請重新產生 DOCKER 設定：

```bash
docker compose up -d mysql
ENV=DEV uv run python genenv.py
```

接著在 `api/` 安裝相依套件並啟動開發伺服器：

```bash
cd api
uv sync
uv run --env-file ../.env uvicorn app.main:app --reload --host 127.0.0.1 --port 8888
```

本機開發文件網址為 <http://127.0.0.1:8888/docs>；Compose 服務網址為 <http://localhost:8889/docs>。在 `api/` 啟動完成後，可用 `cd ..` 回到專案根目錄。

## Redash Swarm 儀表板

Redash 以獨立的 `home_appliance_redash` Swarm stack 部署，不屬於 `appliance` stack。它使用 `appliance_redash_mysql_network` 連接 Swarm MySQL。部署前先確認 `appliance` stack 和這個 overlay network 已存在，並準備好 Redash PostgreSQL、Redis volumes。

Redash stack 的所有服務都使用 `node.labels.redash == true` 選擇 node。部署前需在目前執行 Redash 的 node 設定 `redash=true`；要移動 Redash 時，將此 label 移至目標 node 後重新部署。PostgreSQL 和 Redis 使用 node-local volumes，移動前必須先把資料備份／還原到目標 node，或改用兩個 node 共用的儲存，否則服務可能啟動在空資料上。

在 Swarm manager 上，將下方節點名稱替換為預定執行 Redash 的節點：

```bash
docker node ls
docker node update --label-add redash=true <node-name>
```

在專案根目錄 `.env` 設定 `REDASH_POSTGRES_PASSWORD`、`REDASH_COOKIE_SECRET`、`REDASH_SECRET_KEY`。全新部署可用以下指令產生隨機值；若已有任一 Redash 設定，指令會停止，避免覆蓋：

```bash
python3 manage_env.py init redash
```

既有 Redash 務必保留原本的值，否則可能無法連接資料庫或讀取設定。PostgreSQL 密碼用十六進位字元可避免連線網址的特殊字元問題。

第一次在全新單節點環境使用 Redash 時，Swarm 設定不會建立資料表。可先在預定執行 Redash 的 manager 節點，以 Compose 初始化空的資料庫：

```bash
REDASH_MYSQL_NETWORK=appliance_redash_mysql_network \
  docker compose --env-file .env -f services/docker-compose-redash-local.yml up -d
REDASH_MYSQL_NETWORK=appliance_redash_mysql_network \
  docker compose --env-file .env -f services/docker-compose-redash-local.yml ps -a
REDASH_MYSQL_NETWORK=appliance_redash_mysql_network \
  docker compose --env-file .env -f services/docker-compose-redash-local.yml logs create_table
```

確認 `create_table` 成功結束且 Redash 可開啟後，才繼續下方的切換步驟。這只適用首次建立空資料；已有資料時不要重建資料庫，請先確認原有 volumes 和 `.env`，並備份。多節點環境需確保 volumes 位於 Redash 實際執行的節點，並在 manager 使用相同的設定部署。

切換現有 Compose Redash 至 Swarm 時，先停止並移除 Compose containers，但**不要加 `-v`**，以保留資料 volumes：

```bash
REDASH_MYSQL_NETWORK=appliance_redash_mysql_network \
  docker compose --env-file .env -f services/docker-compose-redash-local.yml down
```

在 Swarm manager 的專案根目錄驗證設定並部署：

```bash
python3 manage_env.py check home_appliance_redash
python3 manage_env.py deploy home_appliance_redash
docker stack services home_appliance_redash
```

全新 Redash 首次部署完成後，工具會互動式要求輸入管理員姓名、Email 和密碼，再詢問 MySQL 使用者。首次初始化時 Redash web 容器必須位於執行指令的 manager 節點；單節點部署符合此條件，多節點請先將 `redash=true` 設在該 manager。MySQL 使用者直接按 Enter 可使用 `.env` 的帳密，也可輸入已建立的唯讀帳號與密碼。工具會自動建立「價格分析」儀表板、三個圖表／表格與五個查詢；分類和日期下拉選單每天台灣時間 12:30 更新。密碼不會寫入程式碼。

此流程只會初始化**全新的 Redash**。既有 Redash 不會被覆蓋或自動更新；如果初始化中斷，管理員帳號可能已建立，請先檢查網頁與錯誤訊息，不要刪除既有資料庫。新環境需要先建立 MySQL 資料表及匯入 CSV 或執行爬蟲，才會有資料可顯示；沒有資料時儀表板仍會建立，但查詢結果是空的。這個流程建立新的帳號與儀表板，不會還原原環境使用者、其他查詢或設定。

此設定使用 `home_appliance_redash_redash_postgres_data` 和 `home_appliance_redash_redash_redis_data`：

- **全新建立：** 不需要舊備份。依上述步驟初始化空資料庫後，工具會建立新的管理員和專案儀表板。
- **搬移既有環境：** 若要保留原有帳號、查詢、儀表板及其他設定，需安全備份並還原 Redash PostgreSQL、Redis 資料及相關 `.env` 密鑰，並驗證還原結果。建立同名空 volumes 不會搬移舊資料；不要把資料庫備份或密鑰公開上傳。

Redash 網頁為 <http://localhost:5006>。Swarm ingress 的 `5006` 不限於 loopback，遠端部署請限制防火牆來源，不要直接對公網開放。停止 stack 可執行 `docker stack rm home_appliance_redash`；這不會刪除外部資料 volumes。

若 Redash 供多人使用，應另外建立僅具備查詢權限的 MySQL 帳號，不要使用管理或寫入帳號。已有儀表板時，請保留既有 PostgreSQL 資料和金鑰；需要完整搬移既有環境時仍須安全備份並驗證還原。

以上指令在專案根目錄執行。

## 建置與部署 Docker image

主 Compose 使用以下 image 版本，實際版本由 `local.ini` 的 `[DOCKER]` 區段產生至 `.env`：

- Crawler：`jacky325/tibame_crawler`（範例 `DOCKER_IMAGE_VERSION=0.0.8`）
- API：`jacky325/tibame_api`（範例 `API_IMAGE_VERSION=0.0.3`）

### 建置及發布

先更新 `local.ini` 中 `[DOCKER]` 的版本和部署所需設定，產生 `.env` 後建置 image：

```bash
ENV=DOCKER uv run python genenv.py
docker compose build worker api
```

發布至 Docker Hub 前，先登入並推送：

```bash
docker login
docker compose push worker api
```

請確認版本標籤已更新且設定符合部署環境；不要將 `.env` 或真實憑證推送到 GitHub 或 image。

### 在其他主機啟動

在目標主機取得專案程式碼，建立 `local.ini` 並設定 `[DOCKER]` 區段，再產生 `.env`。若 image 為私有，先執行 `docker login`。確認目標主機已開放所需服務連接埠且憑證安全後，拉取並啟動：

```bash
docker compose --profile multi-queue pull
docker compose --profile multi-queue up -d --no-build
docker compose --profile multi-queue ps
```

確認 API 可用：`curl http://localhost:8889/health`。遠端主機的安全群組／防火牆需另行設定；不要將 RabbitMQ、MySQL、phpMyAdmin 或未設定登入保護的 Flower 管理介面直接公開。

## Docker Swarm 本機演練與 GCP 部署準備

以下指令都從 `Home_Appliance_Comparison` 專案目錄執行。應用程式使用獨立的 [`docker-compose.swarm.yml`](./docker-compose.swarm.yml)，包含 API、MySQL、RabbitMQ、Crawler workers、Flower 和 phpMyAdmin。Airflow、Redash 和 Portainer 分別使用獨立的 Swarm stack；Airflow 需依下方步驟另外部署。

### 初始化 Swarm 與部署 Portainer

先確認 Docker 是否已加入 Swarm：

```bash
docker info --format '{{.Swarm.LocalNodeState}}'
```

只有狀態為 `inactive` 時才初始化；同一台機器不需要重複執行：

```bash
docker swarm init
```

部署專案內的 [`portainer.yml`](./portainer.yml)：

```bash
docker stack deploy -c portainer.yml portainer
docker stack services portainer
```

服務就緒後，開啟 <http://localhost:9000>。首次設定需要初始管理員密碼及 Setup token；token 可在本機終端機以 `docker service logs --tail 100 portainer_portainer` 查看，請勿分享 token 或 Swarm join token。本機演練不需要 Edge Compute，可略過該設定。

移除 Portainer stack（保留 named volume）：

```bash
docker stack rm portainer
```

### 部署應用程式 stack（單節點測試／雙節點部署）

本 stack 會自行建立所需的 overlay networks，包含供獨立 Redash worker 使用的 attachable `appliance_redash_mysql_network`。切換至 Swarm 前，請先停止使用相同連接埠的 Compose 服務及任何 Compose scheduler，避免衝突或重複派送；保留資料時不要加 `-v`：

```bash
docker compose --profile multi-queue down
```

本機單節點測試時，在唯一的 node 設定 `api=true`、`airflow=true` 與四個 worker labels：`crawler_airfryer=true`、`crawler_fridge=true`、`crawler_tv=true`、`crawler_vacuum=true`，API、Airflow 和四個 worker 都會排在該 node。之後改為雲端雙節點時，將 `crawler_airfryer=true`、`crawler_fridge=true` 放在第一個 node，並將 `crawler_tv=true`、`crawler_vacuum=true` 放在第二個 node；在預定執行 API 的 node 設定 `api=true`，並只在預定執行 Airflow 的 manager node 設定 `airflow=true`。兩個 node 必須先加入同一個 Swarm。每個服務維持 1 個 replica。Labels 是 node 上的 key/value，不會建立新 node；若缺少服務對應的 label，服務會停在 `Pending`。API 和 workers 使用 `{{.Task.Slot}}` hostname，例如 `api.1`、`airfryer.1`；擴增 replicas 時可依 slot 區分 task。

單節點範例：在 manager 執行，將 `<node-name>` 換成 `docker node ls` 顯示的實際名稱；多節點時請依規劃分別設定：

```bash
docker node ls
docker node update \
  --label-add api=true \
  --label-add airflow=true \
  --label-add crawler_airfryer=true \
  --label-add crawler_fridge=true \
  --label-add crawler_tv=true \
  --label-add crawler_vacuum=true \
  <node-name>
```

使用 `local.ini` 的 `[DOCKER]` 設定更新 `.env` 對應項目；Airflow 和 Redash 的其他設定會保留：

```bash
ENV=DOCKER uv run python genenv.py
```

建置 API 和 worker images（若程式碼沒有更新且本機已有相同版本，可略過）：

```bash
docker compose --env-file .env build api worker
docker image ls jacky325/tibame_api
docker image ls jacky325/tibame_crawler
```

Swarm 的 MySQL 會使用外部 volume `home_appliance_comparison_mysql_data`；若由本機 Compose 遷移，這是既有的 MySQL 資料。部署前請在 MySQL 預定執行的 manager 節點確認 volume 存在，並先備份重要資料；不要用 `docker compose down -v` 或刪除該 volume。

```bash
docker volume inspect home_appliance_comparison_mysql_data
```

若是全新環境且確定不需匯入既有資料，可先建立空 volume；MySQL 首次啟動時會初始化資料庫。空 volume 不會包含其他主機的資料。

```bash
docker volume create home_appliance_comparison_mysql_data
```

Swarm 不會自動讀取 `.env` 的變數；使用專案工具讀取設定、驗證並部署，不會印出機密：

```bash
python3 manage_env.py check appliance
python3 manage_env.py deploy appliance
docker stack services appliance
curl http://127.0.0.1:8889/health
```

等待 `docker stack services appliance` 中的服務副本數都就緒（例如 `1/1`）。此 stack 不再包含 scheduler；每日排程請依下方步驟部署 Airflow。從舊版升級時，單純部署 stack 不一定移除舊服務，務必依照 Airflow 遷移步驟移除舊 scheduler。

RabbitMQ 管理頁沿用舊版網址 <http://localhost:15674>，使用 `.env` 中的 `RABBITMQ_DEFAULT_USER` 和 `RABBITMQ_DEFAULT_PASS` 登入。管理頁發布為節點連接埠 `15674`；AMQP `5672` 仍只供 Swarm 內部服務使用。Swarm ingress 不保證只接受本機連線，雲端部署時請限制防火牆來源，勿對公網開放。

Flower 任務監控頁沿用舊版網址 <http://localhost:5557>，登入帳密使用 `.env` 中的 `WORKER_ACCOUNT` / `WORKER_PASSWORD`。若未登入看到 `Access denied`，代表 Basic Auth 尚未通過，不是服務故障；請在瀏覽器驗證提示輸入上述帳密。Flower 管理頁未額外發布 AMQP 連接埠。Swarm ingress 不保證只接受本機連線，雲端部署時請勿將 `5557` 對公網開放。

Swarm stack 啟動 `airfryer`、`fridge`、`tv`、`vacuum` 四個專用 worker，分別消費 Airflow 派送的分類 queues；不啟動預設 `celery` queue 的通用 `worker`。通用 worker 只在手動執行一般模式的 `producer.py` 時需要；`producer` 和 `producer_multi_queue` 本身都是執行完即結束的手動工具。

### Airflow Swarm 排程

Airflow 使用 `CeleryExecutor`、獨立 MySQL metadata 資料庫和專用 Redis broker，不需掛載 Docker socket。流程分成兩套 Celery：

```text
Airflow scheduler → Airflow Redis → Airflow worker 執行 DAG 派送任務
    → 既有 RabbitMQ → 四個分類爬蟲 worker → MySQL
```

MySQL 的 `airflow` 資料庫儲存 Airflow metadata，與爬蟲資料庫分開；Redis DB 0 儲存 Airflow Celery 任務佇列、DB 1 儲存 Celery 任務結果，不對外發布。四個 DAG 預設啟用，分別於台灣時間 09:00、10:00、11:00、12:00 派送。`catchup=False` 不會補跑所有歷史區間，但 Airflow **首次啟動或重新啟動後可能立即執行最近一期**，與原本的 APScheduler 不同。Airflow 任務成功只表示訊息已送到 RabbitMQ，**不表示爬蟲已完成或成功**；實際執行狀態請查看 Flower 或爬蟲 worker 日誌。

#### 設定檔與檔案位置

- [`airflow/airflow.cfg`](./airflow/airflow.cfg)：執行器、DAG 目錄、時區及介面等非機密設定；未列出的項目使用 Airflow 預設值。
- [`airflow/dags/appliance_crawls.py`](./airflow/dags/appliance_crawls.py)：定義四個 DAG、排程時間及派送任務。
- [`airflow/etl/dispatch.py`](./airflow/etl/dispatch.py)：將爬蟲任務派送至 RabbitMQ；實際爬蟲與寫入 MySQL 的程式仍位於 `crawler/`。
- [`Dockerfile.airflow`](./Dockerfile.airflow)：將設定、DAG 和 ETL 任務實作加入 Airflow image，不會把帳密或 `.env` 打包進 image。
- [`docker-compose.airflow.swarm.yml`](./docker-compose.airflow.swarm.yml)：定義 Airflow scheduler、webserver、worker、Flower、Redis、網路和 volumes；MySQL 由主要 Swarm stack 提供。
- `.env`：Airflow MySQL 連線、密鑰、Flower 及 RabbitMQ 帳密；不可提交或打包到 image。

Airflow 環境變數 `AIRFLOW__區段__選項` 會覆蓋 `airflow.cfg` 的對應設定；不要在兩處設定相同選項，以免修改設定檔後不生效。設定檔、DAG 和 ETL 都打包在 image 中，修改後必須重新建置 image，更新版本並重新部署 Airflow stack；單純修改本機檔案不會更新正在運行的容器。`dags_are_paused_at_creation` 只影響新建立的 DAG，不會改變既有 DAG 在網頁上的暫停狀態。

#### 本機與雲端

目前本機及雲端 VM 使用相同的 [`Dockerfile.airflow`](./Dockerfile.airflow) 與 Swarm 設定，不需要另一份 GCE Dockerfile：差異主要是 image registry、節點標籤、持久儲存、設定檔和防火牆。雲端機器必須可以取得建置並推送的 image；請在部署主機安全產生或提供 `.env`，不要把帳密寫進 Dockerfile。雲端需事先規劃 MySQL volume 的持久化與備份，限制管理介面連線並使用 HTTPS。若未來改用 Cloud Composer 等託管 Airflow，部署方式會不同，需另行評估。

先部署並確認 `appliance` stack 的 MySQL、RabbitMQ 和四個 worker 就緒，並確保只有一個 manager node 設有 `airflow=true`。Airflow 的 Redis、scheduler、webserver、worker 和日誌 volume 都固定在此節點；這是單節點配置，不具高可用性，需自行備份 MySQL 和 Redis volumes。Redis 不設密碼，Swarm 節點及可連接此內部 overlay 網路的服務必須受信任。多節點使用時，Airflow image 必須推送至所有可能執行服務的節點可存取的 registry；如需跨節點執行 Airflow worker，還需另行規劃共用日誌及網路安全，不能只增加副本。

例如在要執行 Airflow 的 manager 節點查詢節點名稱後，使用實際名稱設定標籤：

```bash
docker node ls
docker node update --label-add airflow=true <manager-node-name>
```

首次部署前，先在 `appliance` 的 MySQL 建立獨立的 `airflow` 資料庫（字元集 `utf8mb4`、collation `utf8mb4_unicode_ci`）和專用 `airflow` 使用者，授予該使用者 `airflow` 資料庫權限。不要使用爬蟲資料庫或 root 帳號。MySQL 必須啟用 `explicit_defaults_for_timestamp`。例如可透過 phpMyAdmin 建立資料庫與使用者；密碼建議使用隨機十六進位字元。

全新部署時，使用已建立的 MySQL `airflow` 使用者密碼產生設定。密碼必須是非空十六進位字元；密碼會互動式輸入，不會印在終端機。若任一 Airflow 設定已存在，指令會停止，避免覆蓋既有密鑰：

```bash
python3 manage_env.py init airflow
```

指令會將資料庫連線、Fernet key、web secret 和 Flower 帳密加入根目錄 `.env`。既有部署應透過上述合併指令保留原本的值，**不要重新產生 Fernet key**；請安全備份 `.env`，不可提交或分享。Flower 使用 `FLOWER_BASIC_AUTH`，帳密須為單組非空的 `username:password`。Basic Auth 需要搭配 HTTPS 保護傳輸；Swarm 管理員可查看服務環境變數。

從 PostgreSQL 切換到全新的 MySQL 資料庫不會自動搬移帳號、密碼、DAG 狀態或執行歷史。需保留時，請先備份並規劃遷移；切換前應停止排程，避免重複派送。移除服務不會自動刪除舊 PostgreSQL volume；若仍保留舊版回復設定檔，請安全保管並限制存取。

建置 Airflow image。更新 DAG 或 `airflow.cfg` 後應變更版本並重新建置；將相同版本寫入根目錄 `.env` 的 `AIRFLOW_IMAGE_VERSION`，否則部署會使用預設的 `0.0.3`。多節點部署還要先推送到 registry：

```bash
export AIRFLOW_IMAGE_VERSION=0.0.3
docker build -f Dockerfile.airflow -t "jacky325/tibame_airflow:${AIRFLOW_IMAGE_VERSION}" .
# 多節點部署時另執行：docker push "jacky325/tibame_airflow:${AIRFLOW_IMAGE_VERSION}"
```

**從舊版遷移：先停止並移除舊 Swarm scheduler，再啟動 Airflow。** Airflow 啟動後可能立即派送最近一期；也不要同時啟動 Compose 的 scheduler。以下指令不會刪除 workers 或資料庫：

```bash
if docker service inspect appliance_scheduler >/dev/null 2>&1; then
  docker service scale appliance_scheduler=0
  docker service rm appliance_scheduler
fi
python3 manage_env.py check home_appliance_airflow
python3 manage_env.py deploy home_appliance_airflow
docker stack services home_appliance_airflow
```

若 image 位於私有 registry，在 `python3 manage_env.py deploy home_appliance_airflow` 後加上 `--with-registry-auth`，並確保各節點可拉取 image。首次啟動時 scheduler 會初始化 metadata DB，webserver 和 worker 會等待 migration；若尚未就緒，查看下列日誌。服務顯示 `1/1` 後，在上述 `airflow=true` 的節點上建立管理員（密碼會互動式輸入）：

```bash
docker service logs --tail 100 home_appliance_airflow_scheduler
docker service logs --tail 100 home_appliance_airflow_webserver
docker service logs --tail 100 home_appliance_airflow_worker
docker service logs --tail 100 home_appliance_airflow_airflow-redis
docker service logs --tail 100 home_appliance_airflow_flower
docker exec -it "$(docker ps --filter label=com.docker.swarm.service.name=home_appliance_airflow_webserver --format '{{.ID}}' | head -n 1)" \
  airflow users create --username admin --firstname Airflow --lastname Admin \
  --role Admin --email admin@example.com
```

以建立的帳密登入 <http://localhost:8083>，可查看、暫停或手動觸發各 DAG。手動觸發也會派送爬蟲，可能與定時任務重複。Swarm ingress 會在節點發布 `8083`，雲端部署前應限制防火牆來源並使用 HTTPS，不要直接公開管理介面。

Airflow 專用 Flower 位於 <http://localhost:5556>，使用 `.env` 的 Flower 帳密登入，監控 Airflow Celery worker；既有 <http://localhost:5557> 的 Flower 則監控爬蟲 worker。Swarm ingress 會在所有節點發布 `5556`，並非只限本機；雲端或共享主機應限制防火牆來源並使用 HTTPS，勿直接對公網開放。

停止 Airflow（保留 metadata 與日誌 volumes；排程將停止）：

```bash
docker stack rm home_appliance_airflow
```

### 透過 phpMyAdmin 管理 Swarm MySQL

phpMyAdmin 已包含在 `docker-compose.swarm.yml`，執行上方的 `docker stack deploy` 指令時會一併啟動。服務就緒後開啟 <http://localhost:8082>，使用 `.env` 設定的 MySQL 帳號登入。若一般帳號尚未建立，可用 root 帳號及 `MYSQL_ROOT_PASSWORD` 登入。本機 Compose 的 phpMyAdmin 若已佔用 `8082`，請先停止它。Swarm ingress 會在節點發布 `8082`，不保證只接受本機連線。

若先前已將 phpMyAdmin 以獨立的 `phpmyadmin` Swarm stack 部署，請先執行 `docker stack rm phpmyadmin`，再重新部署 `appliance`，避免兩個服務搶用 `8082`。

停止應用程式 stack（保留 named volumes 中的資料）：

```bash
docker stack rm appliance
```

### 部署到 GCP 前的準備

以下為建立 VM 與連線的準備，**不是完整部署流程**。實際部署前仍需確認 VM 的 CPU、記憶體、磁碟、防火牆及備份規劃。GCP 資源可能產生費用，請先確認價格與預算。

#### 建立 VM

在 [Google Cloud Console 的 Compute Engine 頁面](https://console.cloud.google.com/compute/instances)選擇自己的專案，建立或選擇一台 VM。記下專案 ID、VM 名稱和所在 zone，供下方連線時使用。

#### 安裝並初始化 gcloud

在**本機電腦**依照 [Google Cloud CLI 官方安裝說明](https://docs.cloud.google.com/sdk/docs/install-sdk?hl=zh-tw)選擇適合的作業系統安裝方式。安裝完成後確認版本，並初始化：

```bash
gcloud version
gcloud init
```

依提示登入 Google 帳號、選擇 GCP 專案；預設 region 和 zone 可依 VM 所在位置設定，也可稍後更改。不要分享登入驗證碼、SSH 私鑰或帳號憑證。

#### SSH 連線到 VM

仍在**本機終端機**執行，將以下佔位文字換成自己的 VM 名稱、zone 和專案 ID：

```bash
gcloud compute ssh <VM_NAME> --zone <ZONE> --project <PROJECT_ID>
```

首次連線可能提示建立 SSH 金鑰；建議設定私鑰保護密碼。連線需要適當的 GCP 權限及 VM 的 SSH 網路存取設定。連線成功後的指令會在 **VM 上**執行；輸入 `exit` 可返回本機。

#### Image 與資料準備

GCP 節點無法直接使用本機建置的 images。在有專案程式碼與 Docker 的建置主機上，登入 registry，將版本化的 API 和 worker images 推送至所有 Swarm 節點都能存取的位置：

> **安全提醒：** 主 stack 現在包含 phpMyAdmin；下方部署指令也會啟動它並發布 `8082`。GCP 防火牆不要對公網開放 `8082`，只允許經由受信任的私有網路存取。

```bash
docker login
docker compose --env-file .env build worker api
docker compose --env-file .env push worker api
```

完成 Swarm、節點標籤、網路、資料 volume、專案程式碼及 `.env` 等準備後，在部署主機的 Swarm manager 上執行：

```bash
python3 manage_env.py deploy appliance --with-registry-auth
```

目前 `portainer.yml` 發布 HTTP `9000` 和選用的 `8000` Edge 通道，沒有發布 HTTPS `9443`；雲端部署前應將 Portainer 改為 HTTPS，並限制防火牆來源。不要將 Portainer `9000`/`9443`/`8000`、Agent `9001`、MySQL `3306`、RabbitMQ AMQP `5672`、管理頁 `15674` 或 Flower `5557` 對公網公開。Swarm 節點間必要通訊只允許在私有網路中；Docker Desktop 的 Swarm 也不能直接當作 GCP Swarm 使用。

目前 Swarm 檔案引用本機外部 volume `home_appliance_comparison_mysql_data`；GCP 節點不會自動有這個 volume，直接部署可能失敗。雲端部署前要改成 GCP 節點可用的持久化磁碟／volume，並規劃資料備份與還原。`appliance` stack 會建立 attachable overlay network `appliance_redash_mysql_network`，供獨立 Redash Swarm stack 連到 MySQL。其他主機部署 Redash 前，需先部署 `appliance` 並安全提供根目錄 `.env`。全新建立儀表板不需要舊備份；若要搬移既有 Redash 環境，則需安全備份並還原其 PostgreSQL、Redis 資料及相關密鑰，並驗證還原結果。建立同名空 volumes 不會搬移舊查詢或儀表板資料。RabbitMQ 仍使用 manager node 的 local named volume；這不是高可用或備份方案。確認資料層穩定後，才調整 API 或 worker 副本數：

```bash
docker service scale appliance_worker_airfryer=2
```
