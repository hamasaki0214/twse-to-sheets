"""
增量更新 — 每次處理 1 檔今天尚未更新的股票。
讀取分頁中最後一筆資料日期，只抓該日期之後的月份，以天為單位去重複追加。
"""

import sys
import time
from datetime import datetime

from google_sheets import (
    get_client,
    open_all_spreadsheets,
    find_need_update,
    get_last_date,
    update_status,
    append_stock_data,
)
from twse_scraper import scrape_since


def main():
    gc = get_client()
    sheets = open_all_spreadsheets(gc)

    if not sheets:
        print("找不到任何 stock-list 試算表。")
        return

    result = find_need_update(sheets)
    if result is None:
        print("今天所有股票皆已更新。")
        return

    sheet, row, stock_code, stock_name, last_synced = result
    label = f"{stock_code} {stock_name}"

    # 從分頁讀取實際最後一筆資料日期
    last_date_str = get_last_date(sheet, stock_code)
    if not last_date_str:
        print(f"{label} — 分頁無資料，請先執行全量同步（main.py）")
        return

    since_date = datetime.strptime(last_date_str, "%Y/%m/%d").date()
    print(f"增量更新: {label}（資料最後日期: {last_date_str}）")

    update_status(sheet, row, status="updating")
    start = time.time()

    try:
        rows, fetched_months = scrape_since(stock_code, since_date)
        if not rows:
            raise ValueError("未取得任何資料")

        added = append_stock_data(sheet, stock_code, rows)
        elapsed = time.time() - start
        update_status(sheet, row, status="success", is_synced=True, elapsed=elapsed)

        if added > 0:
            print(f"{label} — 更新完成，新增 {added} 筆，耗時 {elapsed:.0f} 秒")
        else:
            print(f"{label} — 資料已是最新，耗時 {elapsed:.0f} 秒")

    except Exception as exc:
        elapsed = time.time() - start
        print(f"{label} — 更新失敗（耗時 {elapsed:.0f} 秒）: {exc}", file=sys.stderr)
        update_status(sheet, row, status="failed", elapsed=elapsed)
        sys.exit(1)


if __name__ == "__main__":
    main()
