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


def get_client():
    """從環境變數 GOOGLE_SHEETS_SECRET 讀取服務帳戶金鑰並建立 gspread 客戶端。"""
    raw = os.environ.get("GOOGLE_SHEETS_SECRET")
    if not raw:
        sys.exit("錯誤: 環境變數 GOOGLE_SHEETS_SECRET 未設定")
    creds_info = json.loads(raw)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def main():
    gc = get_client()

    # 開啟試算表（請改成你的表格名稱）
    spreadsheet_name = os.environ.get("SPREADSHEET_NAME", "stock-list")
    sh = gc.open(spreadsheet_name)
    worksheet = sh.get_worksheet(0)

    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worksheet.append_row(["自動執行測試", "成功", today])
    print(f"已寫入一筆資料到 [{spreadsheet_name}] ({today})")


if __name__ == "__main__":
    main()
