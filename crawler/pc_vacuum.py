import csv
from datetime import datetime
import os
import random
import time
import bs4 as bs
import urllib.request as req
from playwright.sync_api import sync_playwright
from crawler.config import CSV_OUTPUT_DIR
from crawler.database import initialize_database, save_product

CATEGORY_NAME = "掃地機器人"
CATEGORY_URL = "https://24h.pchome.com.tw/region/DMBL/bestsellers"
OUTPUT_DIR = CSV_OUTPUT_DIR
FILE_NAME = os.path.join(OUTPUT_DIR, "pc_vacuum_top20.csv")


def top20(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        page.wait_for_timeout(3000)
        html_content = page.content()
        browser.close()

    html = bs.BeautifulSoup(html_content, "html.parser")
    links = html.find_all("a", {"class": "c-prodInfoV2__link"}, limit=20)

    products = []
    for index, link in enumerate(links):
        href = link.get("href", "")
        if "/prod/" in href:
            fn_raw = href.split("/prod/")[-1]
            fn = fn_raw.split("?")[0]
            rank = index + 1
            if not any(p["fn"] == fn for p in products):
                products.append({"fn": fn, "rank": rank})
    return products


def tv_data(item_info, category_name, today_str, file_name):
    fn = item_info["fn"]
    rank = item_info["rank"]
    url = f"https://24h.pchome.com.tw/prod/{fn}"

    req_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://24h.pchome.com.tw/",
    }
    request = req.Request(url, headers=req_header)
    max_retries = 3
    content = None

    for attempt in range(max_retries):
        try:
            resp = req.urlopen(request)
            content = resp.read()
            break
        except Exception as e:
            if "429" in str(e) and attempt < max_retries - 1:
                sleep_time = 15 * (attempt + 1)
                print(f"[{category_name}] 第 {rank} 名商品 {fn} 遇到 429，自動暫停 {sleep_time} 秒後重試...")
                time.sleep(sleep_time)
            else:
                print(f"無法讀取商品 {fn}: {e}")
                return

    if not content:
        return

    html = bs.BeautifulSoup(content, "html.parser")
    name = html.find("h1", {"data-regression": "prod_info_name"})
    manufacturer = html.find("span", {"aria-hidden": "true"})
    price = html.find("div", {"data-regression": "prod_redPrice"})
    original_price = html.find("div", {"data-regression": "prodPage_originalPrice"})
    score = html.find(class_="c-ratingIcon__textNumber c-ratingIcon__textNumber--m700GrayDarkest")
    reviews_number = html.find("a", {"data-regression": "prod_reviewBtn"})
    introduction = html.find_all("li", {"class": "c-blockCombine__item c-blockCombine__item--prodSlogan"})

    link = f"https://24h.pchome.com.tw/prod/{fn}"
    product_id = fn

    n = name.text.strip() if name else "none"
    m = manufacturer.text.strip() if manufacturer else "none"
    p = price.text.strip() if price else "none"
    op = original_price.text.strip() if original_price else "none"
    s = score.text.strip() if score else "none"
    r = reviews_number.text.strip() if reviews_number else "none"
    combined_intro = " / ".join([item.text.strip() for item in introduction])

    row_data = [today_str, rank, category_name, n, m, p, op, s, r, combined_intro, link, product_id]

    save_product(row_data)
    with open(file_name, mode="a", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(row_data)

    print(f"[{category_name}] 第 {rank} 名已成功寫入：{n}")


def run():
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"=== 開始執行【{CATEGORY_NAME}】爬蟲 ===")

    initialize_database()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_exists = os.path.exists(FILE_NAME)
    with open(FILE_NAME, mode="a", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(["日期", "排名", "家電分類", "商品名稱", "廠商", "特惠價", "原價", "評分", "評價數", "特色", "連結", "商品編號"])

    products_list = top20(CATEGORY_URL)
    print(f"成功取得 {len(products_list)} 個商品，開始依序爬取詳細資料...")

    for item_info in products_list:
        tv_data(item_info, CATEGORY_NAME, today_str, FILE_NAME)
        time.sleep(random.uniform(3.0, 6.0))

    print(f"\n===【{CATEGORY_NAME}】資料已成功寫入 {FILE_NAME}！ ===")
    return len(products_list)


if __name__ == "__main__":
    run()