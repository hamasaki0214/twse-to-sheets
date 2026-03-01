"""
進入點 — 每次執行只處理一檔未同步的股票。
"""

import sys

from google_sheets import (
    get_client,
    open_spreadsheet,
    find_unsynced_stock,
    update_status,
    write_stock_data,
)
from twse_scraper import scrape_stock, HEADERS


def main():
    gc = get_client()
    sheet = open_spreadsheet(gc)

    result = find_unsynced_stock(sheet)
    if result is None:
        print("所有股票皆已同步，無待處理項目。")
        return

    row, stock_code, stock_name = result
    label = f"{stock_code} {stock_name}"

    # 標記為 syncing
    update_status(sheet, row, status="syncing")

    try:
        rows = scrape_stock(stock_code)
        if not rows:
            raise ValueError("未取得任何資料")

        write_stock_data(sheet, stock_code, HEADERS, rows)
        update_status(sheet, row, status="success", is_synced=True)
        print(f"{label} — 同步完成，共 {len(rows)} 筆資料")

    except Exception as exc:
        print(f"{label} — 同步失敗: {exc}", file=sys.stderr)
        update_status(sheet, row, status="failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
