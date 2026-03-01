"""
TWSE 歷史股價爬蟲 — 改寫自 flask-adminlte-stock/apps/utils/scraping_stock.py
移除 pandas，回傳 list-of-lists。

優化：從最近月份往回抓，連續 3 個月無資料即停止（已超過上市日）。
"""

import time
from datetime import date

import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
DEFAULT_YEARS = 10
# 連續幾個月無資料就停止
NO_DATA_STOP = 5
HEADERS = [
    "日期", "成交股數", "成交金額", "開盤價",
    "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數",
]


def _generate_monthly_dates(years=DEFAULT_YEARS):
    """產生從本月到 *years* 年前的每月第一天列表（新→舊）。"""
    end = date.today().replace(day=1)
    start = end - relativedelta(years=years)
    dates = []
    current = end
    while current >= start:
        dates.append(current)
        current -= relativedelta(months=1)
    return dates


def _convert_roc_to_ad(roc_date_str):
    """將民國日期 (例如 '115/03/01') 轉為西元 '2026/03/01'。"""
    year, month, day = roc_date_str.split("/")
    return f"{int(year) + 1911}/{month}/{day}"


def _fetch_month(stock_code, query_date):
    """
    呼叫 TWSE API 抓取指定月份的日交易資料。

    Returns
    -------
    tuple(list, bool)
        (data_rows, api_ok)
        data_rows — 該月的交易資料列表（可能為空）。
        api_ok — API 是否回應成功（True = 正常回應，即使無資料；False = 網路/伺服器錯誤）。
    """
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
                return body["data"], True
            # API 正常回應但該月無資料（股票未上市）
            return [], True
    except (requests.RequestException, ValueError):
        pass
    return [], False


class ScrapeError(Exception):
    """API 呼叫失敗時拋出。"""


def scrape_stock(stock_code, years=DEFAULT_YEARS):
    """
    抓取 *stock_code* 過去 *years* 年的日交易資料。

    從最近月份往回抓：
    - API 無回應 / 錯誤 → 立即拋出 ScrapeError（整個失敗）
    - 連續 NO_DATA_STOP 個月無資料 → 正常結束（已超過上市日）
    最後依日期排序（舊→新）回傳。

    Returns
    -------
    tuple(list[list[str]], int)
        (rows, fetched_months)
        rows — 每列 9 欄，依日期由舊到新排序。
        fetched_months — 實際呼叫 API 的月份數。
    """
    months = _generate_monthly_dates(years)
    all_rows = []
    consecutive_empty = 0
    fetched_months = 0

    for i, d in enumerate(months):
        if i > 0:
            time.sleep(2)
        rows, api_ok = _fetch_month(stock_code, d)
        fetched_months += 1

        if not api_ok:
            raise ScrapeError(f"API 呼叫失敗：{d.strftime('%Y/%m')}")

        if rows:
            consecutive_empty = 0
            for row in rows:
                row = row[:9]
                row[0] = _convert_roc_to_ad(row[0])
                all_rows.append(row)
        else:
            consecutive_empty += 1
            if consecutive_empty >= NO_DATA_STOP:
                print(f"  連續 {NO_DATA_STOP} 個月無資料，停止往回抓取（{d}）")
                break

    # 依日期排序（舊→新），日期格式 YYYY/MM/DD 可直接字串排序
    all_rows.sort(key=lambda row: row[0])

    return all_rows, fetched_months
