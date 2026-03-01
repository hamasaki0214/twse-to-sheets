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


def get_client():
    """從環境變數 GOOGLE_SHEETS_SECRET 讀取服務帳戶金鑰並建立 gspread 客戶端。"""
    raw = os.environ.get("GOOGLE_SHEETS_SECRET")
    if not raw:
        sys.exit("錯誤: 環境變數 GOOGLE_SHEETS_SECRET 未設定")
    creds_info = json.loads(raw)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def open_spreadsheet(gc):
    """開啟試算表並回傳 Spreadsheet 物件。"""
    return gc.open(SPREADSHEET_NAME)


def find_unsynced_stock(sheet):
    """
    在控制表 (第 0 頁) 找出第一筆 is_synced=FALSE 的股票。

    Returns
    -------
    tuple(int, str, str) | None
        (row_number, stock_code, stock_name)；找不到則回傳 None。
    """
    ws = sheet.get_worksheet(0)
    rows = ws.get_all_values()

    # 跳過標題列（第 0 列）
    for idx, row in enumerate(rows[1:], start=2):  # row 2 起算（1-based）
        is_synced = row[COL_IS_SYNCED - 1].strip().upper()
        if is_synced == "FALSE":
            stock_code = row[COL_STOCK_CODE - 1].strip()
            stock_name = row[COL_STOCK_NAME - 1].strip()
            return idx, stock_code, stock_name

    return None


def update_status(sheet, row, status, is_synced=None):
    """更新控制表指定列的 status（及可選的 is_synced、last_synced）。"""
    ws = sheet.get_worksheet(0)
    ws.update_cell(row, COL_STATUS, status)
    if is_synced is not None:
        ws.update_cell(row, COL_IS_SYNCED, str(is_synced).upper())
        if is_synced:
            ws.update_cell(
                row, COL_LAST_SYNCED,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )


def write_stock_data(sheet, stock_code, headers, rows):
    """
    建立（或清除）以 stock_code 命名的工作表，寫入標題與資料。

    使用 batch_update 一次寫入，減少 API 呼叫次數。
    """
    # 嘗試取得既有工作表，否則新建
    try:
        ws = sheet.worksheet(stock_code)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=stock_code, rows=len(rows) + 1, cols=len(headers))

    # 確保列數足夠
    if ws.row_count < len(rows) + 1:
        ws.resize(rows=len(rows) + 1, cols=len(headers))

    # 組合標題 + 資料，一次寫入
    all_data = [headers] + rows
    ws.update(range_name=f"A1:{_col_letter(len(headers))}{len(all_data)}", values=all_data)


def _col_letter(n):
    """將欄位數字轉為字母（1→A, 9→I）。"""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result
