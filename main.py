"""
進入點 — 每次執行只處理一檔未同步的股票。
"""

import sys
import time

from google_sheets import (
    get_client,
    open_spreadsheet,
    get_sync_progress,
    update_status,
    write_stock_data,
)
from twse_scraper import scrape_stock, HEADERS


def main():
    gc = get_client()
    sheet = open_spreadsheet(gc)

    synced, total, unsynced = get_sync_progress(sheet)
    print(f"同步進度: {synced}/{total}")

    if unsynced is None:
        print("所有股票皆已同步，無待處理項目。")
        return

    row, stock_code, stock_name = unsynced
    label = f"{stock_code} {stock_name}"

    # 標記為 syncing
    update_status(sheet, row, status="syncing")
    start = time.time()

    try:
        rows, fetched_months = scrape_stock(stock_code)
        if not rows:
            raise ValueError("未取得任何資料")

        write_stock_data(gc, stock_code, HEADERS, rows)
        elapsed = time.time() - start
        update_status(sheet, row, status="success", is_synced=True, elapsed=elapsed)
        print(f"{label} — 同步完成，抓取 {fetched_months} 個月，共 {len(rows)} 筆，耗時 {elapsed:.0f} 秒")

    except Exception as exc:
        elapsed = time.time() - start
        print(f"{label} — 同步失敗（耗時 {elapsed:.0f} 秒）: {exc}", file=sys.stderr)
        update_status(sheet, row, status="failed", elapsed=elapsed)
        sys.exit(1)


if __name__ == "__main__":
    main()
