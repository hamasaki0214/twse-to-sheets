"""
Google Sheets 讀寫操作 — stock-list 控制表 + 個股資料表。
"""

import os
import json
import sys
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "stock-list")

# 控制表欄位索引（1-based，對應 gspread cell）
COL_STOCK_CODE = 1
COL_STOCK_NAME = 2
COL_IS_SYNCED = 3
COL_LAST_SYNCED = 4
COL_STATUS = 5
COL_ELAPSED = 6

CONTROL_HEADERS = ["stock_code", "stock_name", "is_synced", "last_synced", "status", "elapsed"]


def get_client():
    """從環境變數 GOOGLE_SHEETS_SECRET 讀取服務帳戶金鑰並建立 gspread 客戶端。"""
    raw = os.environ.get("GOOGLE_SHEETS_SECRET")
    if not raw:
        sys.exit("錯誤: 環境變數 GOOGLE_SHEETS_SECRET 未設定")
    creds_info = json.loads(raw)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def open_all_spreadsheets(gc):
    """
    開啟所有 stock-list 系列試算表（stock-list, stock-list_1, stock-list_2, ...）。

    Returns
    -------
    list[Spreadsheet]
        找到的所有試算表列表。
    """
    sheets = []

    # 先開 stock-list
    try:
        sheets.append(gc.open(SPREADSHEET_NAME))
    except gspread.exceptions.SpreadsheetNotFound:
        return sheets

    # 依序嘗試 stock-list_1, stock-list_2, ...
    n = 1
    while True:
        try:
            sheets.append(gc.open(f"{SPREADSHEET_NAME}_{n}"))
            n += 1
        except gspread.exceptions.SpreadsheetNotFound:
            break

    return sheets


def get_sync_progress(sheets):
    """
    掃描所有試算表的控制表，回傳總進度與第一筆未同步股票。

    Returns
    -------
    tuple(int, int, tuple | None)
        (synced_count, total_count, unsynced_stock)
        unsynced_stock — (sheet, row_number, stock_code, stock_name) 或 None。
    """
    total = 0
    synced = 0
    first_unsynced = None

    for sheet in sheets:
        ws = sheet.get_worksheet(0)
        rows = ws.get_all_values()

        data_rows = rows[1:]
        total += len(data_rows)

        for idx, row in enumerate(data_rows, start=2):
            is_synced = row[COL_IS_SYNCED - 1].strip().upper()
            if is_synced == "TRUE":
                synced += 1
            elif first_unsynced is None:
                stock_code = row[COL_STOCK_CODE - 1].strip()
                stock_name = row[COL_STOCK_NAME - 1].strip()
                first_unsynced = (sheet, idx, stock_code, stock_name)

    return synced, total, first_unsynced


def update_status(sheet, row, status, is_synced=None, elapsed=None):
    """更新控制表指定列的 status（及可選的 is_synced、last_synced、elapsed）。"""
    ws = sheet.get_worksheet(0)
    ws.update_cell(row, COL_STATUS, status)
    if elapsed is not None:
        ws.update_cell(row, COL_ELAPSED, f"{elapsed:.0f}s")
    if is_synced is not None:
        ws.update_cell(row, COL_IS_SYNCED, str(is_synced).upper())
        if is_synced:
            ws.update_cell(
                row, COL_LAST_SYNCED,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )


def write_stock_data(sheet, stock_code, headers, rows):
    """
    在 stock-list 試算表中建立（或清除）以 stock_code 命名的分頁，寫入資料。

    寫入後驗證列數是否正確，否則拋出例外。
    """
    # 嘗試取得既有分頁，否則新建
    try:
        ws = sheet.worksheet(stock_code)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=stock_code, rows=len(rows) + 1, cols=len(headers))
        print(f"  已建立分頁: {stock_code}")

    # 確保列數足夠
    total_rows = len(rows) + 1
    if ws.row_count < total_rows:
        ws.resize(rows=total_rows, cols=len(headers))

    # 組合標題 + 資料，一次寫入
    all_data = [headers] + rows
    ws.update(range_name=f"A1:{_col_letter(len(headers))}{total_rows}", values=all_data)

    # 驗證寫入結果
    actual = ws.row_count
    if actual < total_rows:
        raise RuntimeError(
            f"寫入驗證失敗：分頁 {stock_code} 預期至少 {total_rows} 列，實際 {actual} 列"
        )


def _col_letter(n):
    """將欄位數字轉為字母（1→A, 9→I）。"""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result
