"""
列出並清理服務帳戶 Google Drive 中的所有檔案。
"""

from google_sheets import get_client


def main():
    gc = get_client()
    files = gc.list_spreadsheet_files()

    if not files:
        print("服務帳戶的 Drive 中沒有任何試算表。")
        return

    print(f"共找到 {len(files)} 個試算表:\n")
    for f in files:
        print(f"  {f['id']}  {f['name']}")

    print()
    answer = input("是否刪除全部檔案？(y/N): ").strip().lower()
    if answer != "y":
        print("取消刪除。")
        return

    for f in files:
        gc.del_spreadsheet(f["id"])
        print(f"  已刪除: {f['name']}")

    print(f"\n已清理 {len(files)} 個檔案。")


if __name__ == "__main__":
    main()
