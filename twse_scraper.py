"""
TWSE 歷史股價爬蟲 — 改寫自 flask-adminlte-stock/apps/utils/scraping_stock.py
移除 pandas，回傳 list-of-lists。
"""

import time
from datetime import date

import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
DEFAULT_YEARS = 10
HEADERS = [
    "日期", "成交股數", "成交金額", "開盤價",
    "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數",
]


def _generate_monthly_dates(years=DEFAULT_YEARS):
    """產生從 *years* 年前到本月的每月第一天列表。"""
    start = date.today().replace(day=1) - relativedelta(years=years)
    today = date.today()
    dates = []
    current = start
    while current <= today:
        dates.append(current)
        current += relativedelta(months=1)
    return dates


def _convert_roc_to_ad(roc_date_str):
    """將民國日期 (例如 '115/03/01') 轉為西元 '2026/03/01'。"""
    year, month, day = roc_date_str.split("/")
    return f"{int(year) + 1911}/{month}/{day}"


def _fetch_month(stock_code, query_date):
    """呼叫 TWSE API 抓取指定月份的日交易資料，回傳 list-of-lists 或空 list。"""
    url = (
        f"{BASE_URL}?response=json"
        f"&date={query_date.strftime('%Y%m%d')}"
        f"&stockNo={stock_code}"
    )
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            body = resp.json()
            if body.get("stat") == "OK" and "data" in body:
                return body["data"]
    except (requests.RequestException, ValueError):
        pass
    return []


def scrape_stock(stock_code, years=DEFAULT_YEARS):
    """
    抓取 *stock_code* 過去 *years* 年的日交易資料。

    Returns
    -------
    list[list[str]]
        每列 9 欄：日期(西元)、成交股數、成交金額、開盤價、
        最高價、最低價、收盤價、漲跌價差、成交筆數。
    """
    months = _generate_monthly_dates(years)
    all_rows = []

    for i, d in enumerate(months):
        if i > 0:
            time.sleep(2)
        rows = _fetch_month(stock_code, d)
        for row in rows:
            # ETF 可能回傳 10 欄，截取前 9 欄
            row = row[:9]
            row[0] = _convert_roc_to_ad(row[0])
            all_rows.append(row)

    return all_rows
