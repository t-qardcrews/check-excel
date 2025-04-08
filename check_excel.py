import json
import os
import shutil
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

MESSAGE_HEADER = "【ここにメッセージヘッダーを書く】\n"
service_account_info = json.loads(os.environ["GDRIVE_CREDENTIALS"])
scope = ["https://www.googleapis.com/auth/drive"]
gauth = GoogleAuth()
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    service_account_info, scope
)
drive = GoogleDrive(gauth)

DOWNLOAD_DIR = Path("./temp_download")
DOWNLOAD_DIR.mkdir(exist_ok=True)
SHARED_DRIVE_ID = os.environ.get("SHARED_DRIVE_ID")
if not SHARED_DRIVE_ID or SHARED_DRIVE_ID == "SHARED_DRIVE_ID":
    raise ValueError(
        "環境変数 SHARED_DRIVE_ID が正しく設定されていません。実際の共有ドライブのIDを設定してください。"
    )
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")


def get_folder_id_by_name(shared_drive_id: str, folder_name: str) -> str:
    query = "mimeType='application/vnd.google-apps.folder' and title='{}' and trashed=false".format(
        folder_name
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
            "フォルダ '{}' が共有ドライブ内に見つかりませんでした。".format(folder_name)
        )
    return folder_list[0]["id"]


def list_excel_files_in_subfolders(
    shared_drive_id: str, parent_folder_id: str
) -> list[dict]:
    """
    指定されたフォルダ (parent_folder_id) の直下にあるすべてのサブフォルダをリストアップし、
    それぞれのサブフォルダ内から、タイトルに【 と 】を含む Excel ファイルを取得する。
    ゴミ箱内のファイルは除外するため、各クエリに trashed=false を追加しています。
    """
    subfolder_query = (
        "'{}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    ).format(parent_folder_id)
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
            "and title contains '【' and title contains '】' and trashed=false"
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
    共有ドライブ内から、まず「出勤簿」フォルダを取得し、
    その中の「202503(test)」フォルダを探します。
    その上で、「202503(test)」フォルダ直下のすべてのサブフォルダから、
    タイトルに【 と 】を含む Excel ファイルをリストアップします。
    ゴミ箱内のフォルダ・ファイルは除外するため、trashed=false を追加しています。
    """
    shukkin_folder_id = get_folder_id_by_name(shared_drive_id, "出勤簿")

    query = (
        "mimeType='application/vnd.google-apps.folder' and title='202503(test)' and "
        "'{}' in parents and trashed=false"
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

    excel_files = list_excel_files_in_subfolders(shared_drive_id, target_folder_id)
    return excel_files


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


####################################
# 制約条件
####################################


# 同一時間に複数財源で勤務してはいけない
def check_group_for_overlaps(group: pd.DataFrame) -> list[str]:
    """Check overlapping intervals in a group."""
    group_sorted = group.sort_values("start")
    messages = []
    for i in range(len(group_sorted) - 1):
        current_row = group_sorted.iloc[i]
        next_row = group_sorted.iloc[i + 1]
        if current_row["end"] >= next_row["start"]:
            msg1 = f"[勤務時間重複] {current_row['start'].date()} - {current_row['file_name']}"
            msg2 = (
                f"[勤務時間重複] {next_row['start'].date()} - {next_row['file_name']}"
            )
            messages.append(msg1)
            messages.append(msg2)
    return messages


# 連続勤務は5日まで
def check_group_for_up_to_5_consecutive_working_days(group: pd.DataFrame) -> list[str]:
    errors = []

    if group.empty:
        return errors

    # 開始時刻でソートし、各行の勤務日(date部分)を抽出する
    group = group.sort_values("start").copy()
    group["work_date"] = group["start"].dt.date

    # ユニークな勤務日を昇順でリスト化
    unique_dates = sorted(group["work_date"].unique())
    continuous_blocks = []  # 連続勤務日のブロック（set形式で保持）

    # 連続日チェック：連続する勤務日のリストblock_datesを作成する
    if unique_dates:
        block_dates = [unique_dates[0]]
        prev_date = unique_dates[0]
        for current_date in unique_dates[1:]:
            if (current_date - prev_date).days == 1:
                block_dates.append(current_date)
            else:
                if len(block_dates) > 5:  # 連続勤務日数が5日を超過している場合のみ記録
                    continuous_blocks.append(set(block_dates))
                block_dates = [current_date]
            prev_date = current_date
        # 最終ブロックのチェック
        if len(block_dates) > 5:
            continuous_blocks.append(set(block_dates))

    # 各レコードが、連続勤務ブロックに含まれる日付かどうかを確認する
    for _, row in group.iterrows():
        for block in continuous_blocks:
            if row["work_date"] in block:
                errors.append(
                    f"[連続5日超過] {row['start'].date()} - {row['file_name']}"
                )
                break  # 同一レコードについては1回だけエラー出力すればよい
    return errors


# 連続勤務は6時間まで
def check_group_for_up_to_6_consecutive_working_hours(group: pd.DataFrame) -> list[str]:
    errors = []

    if group.empty:
        return errors

    # Sort by start time
    group = group.sort_values("start").copy()

    # Calculate the cumulative working hours within a 6-hour window
    group["cumulative_hours"] = 0
    for i in range(len(group)):
        current_start = group.iloc[i]["start"]
        six_hour_window = group[
            (group["start"] >= current_start)
            & (group["start"] < current_start + pd.Timedelta(hours=6))
        ]
        total_hours = (six_hour_window["end"] - six_hour_window["start"]).sum()
        group.at[group.index[i], "cumulative_hours"] = (
            total_hours.total_seconds() / 3600
        )

    # Check for violations
    for _, row in group.iterrows():
        if row["cumulative_hours"] > 6:
            errors.append(f"[連続6時間超過] {row['start'].date()} - {row['file_name']}")

    return errors


# 1週間あたり28時間まで
def check_group_for_up_to_28_working_hours_per_week(group: pd.DataFrame) -> list[str]:
    errors = []

    if group.empty:
        return errors

    # Add a column for the week number
    group = group.copy()
    group["week"] = group["start"].dt.to_period("W")

    # Find weeks that exceed 28 hours
    weekly_hours = group.groupby("week").apply(
        lambda x: (x["end"] - x["start"]).sum().total_seconds() / 3600,
        include_groups=False,
    )

    # Get all weeks that exceed 28 hours
    violated_weeks = weekly_hours[weekly_hours > 28].index.tolist()

    # For each violated week, add an error message for every file in that week
    for week in violated_weeks:
        week_records = group[group["week"] == week]
        for _, row in week_records.iterrows():
            errors.append(f"[週28時間超過] {row['start'].date()} - {row['file_name']}")

    return errors


# 勤務時間は8:30-17:15まで
def check_group_for_8_30_to_17_15(group: pd.DataFrame) -> list[str]:
    """Check if working hours are within the allowed time frame (8:30 - 17:15)."""
    errors = []

    # Define the allowed time frame
    allowed_start = pd.Timestamp("1900-01-01 08:30:00").time()
    allowed_end = pd.Timestamp("1900-01-01 17:15:00").time()

    for _, row in group.iterrows():
        # Extract time component of the datetime
        start_time = row["start"].time()
        end_time = row["end"].time()

        # Check if start time is before allowed start or end time is after allowed end
        if start_time < allowed_start or end_time > allowed_end:
            errors.append(f"[時間外勤務] {row['start'].date()} - {row['file_name']}")

    return errors


def extract_errors_from_group(group: pd.DataFrame) -> set:
    error_messages = []
    error_messages.extend(check_group_for_overlaps(group))
    error_messages.extend(check_group_for_up_to_5_consecutive_working_days(group))
    error_messages.extend(check_group_for_up_to_6_consecutive_working_hours(group))
    error_messages.extend(check_group_for_up_to_28_working_hours_per_week(group))
    error_messages.extend(check_group_for_8_30_to_17_15(group))
    return error_messages


def extract_errors_from_standard_df(df_standard: pd.DataFrame) -> set:
    df = df_standard.copy()
    df["name_and_name_kana"] = df["name"] + "-" + df["name_kana"]

    error_messages = []
    for _, group in df.groupby("name_and_name_kana"):
        error_messages.extend(extract_errors_from_group(group))

    return set(error_messages)


def extract_name_from_line(line: str) -> str:
    """
    エラーメッセージの行からファイル名の末尾部分を抽出
    エラーメッセージは以下のスタイルで統一する
    [エラータイプ] 勤務日 - ファイル名
    例: [勤務時間重複] 2025-03-28 - 【3月勤務】AA出勤簿Ver.2.1(大関運営費_あいうえお).xlsx
    """
    try:
        file_name = line.split("]")[1].strip().split(" - ")[-1]
    except IndexError:
        file_name = line
    base = file_name.rsplit(".", 1)[0]
    if "_" in base:
        extracted = base.rsplit("_", 1)[-1].strip("()")
        return extracted if extracted else "その他"
    return "その他"


def send_slack_notification(message: str):
    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK, json=payload)
    if response.status_code != 200:
        print("Slack 通知に失敗しました:", response.text)


def group_errors_by_name(error_message_formatted: str) -> str:
    """
    エラーメッセージ全体を改行で分割し、各行から上記の方法で抽出した
    ファイル名末尾部分をキーとしてグループ化し、各グループごとにまとめたレポート文字列を返す。
    """
    error_lines = error_message_formatted.splitlines()
    groups = defaultdict(list)

    for line in error_lines:
        name = extract_name_from_line(line)
        groups[name].append(line)

    result_lines = []
    for name, lines in groups.items():
        lines.sort()
        result_lines.append(f"■ {name} のエラー")
        for err_line in lines:
            result_lines.append("  " + err_line)
        result_lines.append("")  # グループ間の空行
    return "\n".join(result_lines)


def main():
    # 使用例：対象のExcelファイル一覧を取得
    drive_file_list = list_excel_files_in_folder(SHARED_DRIVE_ID)

    if not drive_file_list:
        print("対象フォルダ内にExcelファイルが見つかりませんでした。")
    else:
        print("取得したファイル一覧:")
        for file in drive_file_list:
            print(f"タイトル: {file['title']}, ID: {file['id']}")

    path_list = download_files(drive_file_list, DOWNLOAD_DIR)
    print("\nダウンロードしたファイルパス一覧:")
    for path in path_list:
        print(path)

    df_standard = create_standard_dataframe(path_list)
    error_message_set = extract_errors_from_standard_df(df_standard)
    # もともとの error メッセージを結合
    error_message_formatted = "\n".join(error_message_set)

    # グループ化したエラーメッセージに置き換え
    grouped_error_message = group_errors_by_name(error_message_formatted)

    if grouped_error_message != "":
        message = "出勤簿に入力ミスがあります。\n" + grouped_error_message
    else:
        message = "Excel チェックは正常に終了しました。"

    message = MESSAGE_HEADER + "\n" + message
    print(message)
    send_slack_notification(message)

    shutil.rmtree(DOWNLOAD_DIR)


if __name__ == "__main__":
    main()
