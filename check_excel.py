import json
import os
import shutil
from pathlib import Path
from pprint import pprint

import numpy as np
import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# 環境変数 "GDRIVE_CREDENTIALS" に JSON 文字列としてサービスアカウント情報が保存されている前提
service_account_info = json.loads(os.environ["GDRIVE_CREDENTIALS"])

# 必要なスコープを指定
scope = ["https://www.googleapis.com/auth/drive"]

# GoogleAuth の初期化
gauth = GoogleAuth()

# JSON の内容から認証情報を作成
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    service_account_info, scope
)

# GoogleDrive のインスタンスを作成
drive = GoogleDrive(gauth)


# -----------------------------------------------
# ② 共有ドライブ上の対象フォルダからExcelファイルを取得
# -----------------------------------------------

# 環境変数 PARENT_FOLDER_ID に対象フォルダのIDを設定しておく（例："0Axxxxxxx"）
# SHARED_DRIVE_ID = os.environ.get("SHARED_DRIVE_ID", "SHARED_DRIVE_ID")

# 一時的にダウンロードするディレクトリ
DOWNLOAD_DIR = Path("./temp_download")
DOWNLOAD_DIR.mkdir(exist_ok=True)


# 共有ドライブのIDを環境変数から取得
SHARED_DRIVE_ID = os.environ.get("SHARED_DRIVE_ID")
if not SHARED_DRIVE_ID or SHARED_DRIVE_ID == "SHARED_DRIVE_ID":
    raise ValueError(
        "環境変数 SHARED_DRIVE_ID が正しく設定されていません。実際の共有ドライブのIDを設定してください。"
    )


def get_folder_id_by_name(shared_drive_id: str, folder_name: str) -> str:
    """
    共有ドライブ内から、指定したフォルダ名（完全一致）のフォルダのIDを返す。
    複数見つかった場合は最初のものを返す。
    ※Drive API v2 では、ファイル名のフィールドは title です。
    """
    query = (
        "mimeType='application/vnd.google-apps.folder' and title='{}'".format(
            folder_name
        )
    )
    folder_list = drive.ListFile(
        {
            "q": query,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": shared_drive_id,
            "corpora": "drive",
        }
    ).GetList()

    if not folder_list:
        raise Exception(
            "フォルダ '{}' が共有ドライブ内に見つかりませんでした。".format(
                folder_name
            )
        )
    return folder_list[0]["id"]


def list_excel_files_in_subfolders(
    shared_drive_id: str, parent_folder_id: str
) -> list[dict]:
    """
    指定されたフォルダ (parent_folder_id) の直下にあるすべてのサブフォルダをリストアップし、
    それぞれのサブフォルダ内から、タイトルに【 と 】を含む Excel ファイルを取得する。
    """
    # サブフォルダのクエリ（直下のフォルダ）
    subfolder_query = "'{}' in parents and mimeType='application/vnd.google-apps.folder'".format(
        parent_folder_id
    )
    subfolders = drive.ListFile(
        {
            "q": subfolder_query,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": shared_drive_id,
            "corpora": "drive",
        }
    ).GetList()

    excel_files = []
    for subfolder in subfolders:
        subfolder_id = subfolder["id"]
        file_query = (
            "'{}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
            "and title contains '【' and title contains '】'"
        ).format(subfolder_id)
        files = drive.ListFile(
            {
                "q": file_query,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
                "driveId": shared_drive_id,
                "corpora": "drive",
            }
        ).GetList()
        excel_files.extend(files)
    return excel_files


def list_excel_files_in_folder(shared_drive_id: str) -> list[dict]:
    """
    共有ドライブ（例: T-QARD）内から、まず「出勤簿」フォルダを取得し、
    その中の「202503(test)」フォルダを探します。
    その上で、「202503(test)」フォルダ直下のすべてのサブフォルダから、
    タイトルに【 と 】を含む Excel ファイルをリストアップします。
    """
    # ① 「出勤簿」フォルダのIDを取得
    shukkin_folder_id = get_folder_id_by_name(shared_drive_id, "★出勤簿")

    # ② 「出勤簿」フォルダ内から、タイトルが「202503(test)」のフォルダを検索
    query = (
        "mimeType='application/vnd.google-apps.folder' and title='202503(test)' and "
        "'{}' in parents"
    ).format(shukkin_folder_id)
    folder_list = drive.ListFile(
        {
            "q": query,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": shared_drive_id,
            "corpora": "drive",
        }
    ).GetList()

    if not folder_list:
        raise Exception(
            "フォルダ '202503(test)' が '出勤簿' 内に見つかりませんでした。"
        )
    target_folder_id = folder_list[0]["id"]

    # ③ 「202503(test)」フォルダ内のサブフォルダから Excel ファイルを取得
    excel_files = list_excel_files_in_subfolders(
        shared_drive_id, target_folder_id
    )
    return excel_files


# 使用例
drive_file_list = list_excel_files_in_folder(SHARED_DRIVE_ID)


# 取得したファイル一覧
# drive_file_list = list_excel_files_in_folder(PARENT_FOLDER_ID)
drive_file_list = list_excel_files_in_folder(os.environ.get("SHARED_DRIVE_ID"))


if not drive_file_list:
    print("対象フォルダ内にExcelファイルが見つかりませんでした。")
else:
    print("取得したファイル一覧:")
    for file in drive_file_list:
        print(f"タイトル: {file['title']}, ID: {file['id']}")


# ダウンロードしてローカルパスのリストを作成
def download_files(file_list: list[dict], download_dir: Path) -> list[Path]:
    local_paths = []
    for file in file_list:
        local_path = download_dir / file["title"]
        file.GetContentFile(
            str(local_path),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        local_paths.append(local_path)
    return local_paths


path_list = download_files(drive_file_list, DOWNLOAD_DIR)
print("\nダウンロードしたファイルパス一覧:")
for path in path_list:
    print(path)

# -----------------------------------------------
# ③ Excel ファイルの内容チェック用関数群 (元のコードを流用)
# -----------------------------------------------


def extract_name(df_raw: pd.DataFrame) -> tuple[str, str]:
    name = str(df_raw[2].iat[2])
    name_kana = str(df_raw[2].iat[1])
    name = "".join(name.split())
    name_kana = "".join(name_kana.split())
    return name, name_kana


def extract_date(df_raw: pd.DataFrame) -> pd.DataFrame:
    date_arr = np.concatenate([df_raw[0][5:37].values, df_raw[7][5:35].values])
    date = pd.Series(date_arr)
    start = pd.concat([df_raw[2][5:37], df_raw[9][5:35]], ignore_index=True)
    end = pd.concat([df_raw[4][5:37], df_raw[11][5:35]], ignore_index=True)
    remarks = pd.concat([df_raw[6][5:37], df_raw[13][5:35]], ignore_index=True)
    remarks = remarks.fillna("")

    date.iloc[1::2] = date.iloc[0::2]
    remarks.iloc[1::2] = remarks.iloc[0::2]

    date = pd.to_datetime(date, errors="coerce")
    start = pd.to_timedelta(start, errors="coerce")
    end = pd.to_timedelta(end, errors="coerce")

    df = pd.DataFrame(
        {
            "start": date + start,
            "end": date + end,
            "remarks": remarks,
        }
    )
    df = df.dropna(subset=["start", "end"], how="all").reset_index(drop=True)
    return df


def extract_project_code(df_raw: pd.DataFrame) -> str:
    project_code = df_raw[1].iat[42]
    return project_code


def extract_employment_type(df_raw: pd.DataFrame) -> str:
    employment_type = df_raw[0].iat[0]
    if employment_type == "アドミニストレイティブ・アシスタント出勤簿":
        return "AA"
    elif employment_type == "ティーチング・アシスタント出勤簿":
        return "TA"
    elif employment_type == "リサーチ・アシスタント出勤簿":
        return "RA"
    else:
        return ""


def extract_subject(df_raw: pd.DataFrame) -> str:
    subject = df_raw[1].iat[44]
    if pd.isna(subject):
        subject = ""
    return "".join(subject.split())


def create_standard_dataframe_single(path: Path) -> pd.DataFrame:
    df_raw = pd.read_excel(path, sheet_name="出勤簿様式", header=None)

    name, name_kana = extract_name(df_raw)
    date_df = extract_date(df_raw)
    project_code = extract_project_code(df_raw)
    employment_type = extract_employment_type(df_raw)
    subject = extract_subject(df_raw)

    df = pd.DataFrame(date_df)
    df["name"] = name
    df["name_kana"] = name_kana
    df["project_code"] = project_code
    df["employment_type"] = employment_type
    df["subject"] = subject
    df["file_name"] = path.name

    df_standard = df[
        [
            "name",
            "name_kana",
            "start",
            "end",
            "remarks",
            "project_code",
            "subject",
            "employment_type",
            "file_name",
        ]
    ]
    return df_standard


def sort_df_standard(df_standard: pd.DataFrame) -> pd.DataFrame:
    df_standard.sort_values("start", inplace=True)
    df_standard.sort_values("name_kana", inplace=True)
    df_standard.reset_index(drop=True, inplace=True)
    return df_standard


def create_standard_dataframe(path_list: list[Path]) -> pd.DataFrame:
    df_standard_list = []
    for path in path_list:
        if not isinstance(path, Path):
            path = Path(path)
        df_standard_list.append(create_standard_dataframe_single(path))
    df_standard = pd.concat(df_standard_list, ignore_index=True)
    df_standard = sort_df_standard(df_standard)
    return df_standard


def check_overlapping_intervals(df: pd.DataFrame) -> list[str]:
    df["name_and_name_kana"] = df["name"] + "-" + df["name_kana"]
    error_messages = []
    for key, group in df.groupby("name_and_name_kana"):
        group_sorted = group.sort_values("start")
        for i in range(len(group_sorted) - 1):
            current_row = group_sorted.iloc[i]
            next_row = group_sorted.iloc[i + 1]
            if current_row["end"] >= next_row["start"]:
                msg1 = f"[勤務時間重複] {current_row['file_name']} - {current_row['start']} - {current_row['end']}"
                msg2 = f"[勤務時間重複] {next_row['file_name']} - {next_row['start']} - {next_row['end']}"
                error_messages.append(msg1)
                error_messages.append(msg2)
    return error_messages


def extract_errors_from_standard_df(df_standard: pd.DataFrame) -> set:
    error_message_list = check_overlapping_intervals(df_standard)
    error_message_set = set(error_message_list)  # 重複排除のため set に変換
    return error_message_set


# -----------------------------------------------
# ④ Slack 通知用の関数
# -----------------------------------------------

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")


def send_slack_notification(message: str):
    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK, json=payload)
    if response.status_code != 200:
        print("Slack 通知に失敗しました:", response.text)


# -----------------------------------------------
# ⑤ メイン処理: Excel ファイルのチェック
# -----------------------------------------------

df_standard = create_standard_dataframe(path_list)
error_message_set = extract_errors_from_standard_df(df_standard)
error_message_formatted = "\n".join(error_message_set)

if error_message_formatted != "":
    send_slack_notification(
        "出勤簿に入力ミスがあります。\n" + error_message_formatted
    )
    raise ValueError(
        "出勤簿に入力ミスがあります。\n" + error_message_formatted
    )
else:
    print("全ての出勤簿に入力ミスはありませんでした。")
    send_slack_notification("Excel チェックは正常に終了しました。")

# -----------------------------------------------
# ⑥ 一時ディレクトリのクリーンアップ
# -----------------------------------------------
shutil.rmtree(DOWNLOAD_DIR)
