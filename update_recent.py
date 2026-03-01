"""
增量更新 — 所有已同步的股票，抓最近 2 個月資料追加。
"""

import sys
import time

from google_sheets import (
    get_client,
    open_all_spreadsheets,
    get_all_synced,
    update_status,
    append_stock_data,
)
from twse_scraper import scrape_recent


def main():
    gc = get_client()
    sheets = open_all_spreadsheets(gc)

    if not sheets:
        print("找不到任何 stock-list 試算表。")
        return

    stocks = get_all_synced(sheets)
    if not stocks:
        print("沒有已同步的股票可更新。")
        return

    print(f"共 {len(stocks)} 檔股票需要更新")

    success = 0
    failed = 0

    for sheet, row, stock_code, stock_name in stocks:
        label = f"{stock_code} {stock_name}"
        start = time.time()

        try:
            rows, fetched_months = scrape_recent(stock_code)
            if not rows:
                print(f"{label} — 無新資料")
                continue

            added = append_stock_data(sheet, stock_code, rows)
            elapsed = time.time() - start
            update_status(sheet, row, status="success", is_synced=True, elapsed=elapsed)

            if added > 0:
                print(f"{label} — 新增 {added} 筆，耗時 {elapsed:.0f} 秒")
            else:
                print(f"{label} — 資料已是最新")
            success += 1

        except Exception as exc:
            elapsed = time.time() - start
            print(f"{label} — 更新失敗（耗時 {elapsed:.0f} 秒）: {exc}", file=sys.stderr)
            update_status(sheet, row, status="failed", elapsed=elapsed)
            failed += 1

    print(f"\n更新完成: 成功 {success}，失敗 {failed}")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
