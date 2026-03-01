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


def open_spreadsheet(gc):
    """開啟試算表並回傳 Spreadsheet 物件，若不存在則自動建立並寫入標題列。"""
    try:
        sheet = gc.open(SPREADSHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        sheet = gc.create(SPREADSHEET_NAME)
        print(f"已自動建立試算表: {SPREADSHEET_NAME}")

    # 確保控制表有標題列
    ws = sheet.get_worksheet(0)
    first_row = ws.row_values(1)
    if not first_row:
        ws.update(range_name=f"A1:{_col_letter(len(CONTROL_HEADERS))}1", values=[CONTROL_HEADERS])
        ws.update_title("stock-list")
        print("已自動建立 stock-list 標題列")

    return sheet


def get_sync_progress(sheet):
    """
    回傳控制表的同步進度。

    Returns
    -------
    tuple(int, int, tuple | None)
        (synced_count, total_count, unsynced_stock)
        unsynced_stock — (row_number, stock_code, stock_name) 或 None。
    """
    ws = sheet.get_worksheet(0)
    rows = ws.get_all_values()

    data_rows = rows[1:]  # 跳過標題列
    total = len(data_rows)
    synced = 0
    first_unsynced = None

    for idx, row in enumerate(data_rows, start=2):  # row 2 起算（1-based）
        is_synced = row[COL_IS_SYNCED - 1].strip().upper()
        if is_synced == "TRUE":
            synced += 1
        elif first_unsynced is None:
            stock_code = row[COL_STOCK_CODE - 1].strip()
            stock_name = row[COL_STOCK_NAME - 1].strip()
            first_unsynced = (idx, stock_code, stock_name)

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


def write_stock_data(gc, stock_code, headers, rows):
    """
    建立（或開啟）以 stock_code 命名的獨立試算表，寫入標題與資料。

    寫入後驗證列數是否正確，否則拋出例外。
    """
    # 嘗試開啟既有試算表，否則新建
    try:
        sp = gc.open(stock_code)
    except gspread.exceptions.SpreadsheetNotFound:
        sp = gc.create(stock_code)
        print(f"  已建立試算表: {stock_code}")

    ws = sp.get_worksheet(0)
    ws.clear()

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
            f"寫入驗證失敗：試算表 {stock_code} 預期至少 {total_rows} 列，實際 {actual} 列"
        )


def _col_letter(n):
    """將欄位數字轉為字母（1→A, 9→I）。"""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result
