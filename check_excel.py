import datetime
import json
import os
import re
import shutil
from collections import defaultdict
from pathlib import Path
from pprint import pprint

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

load_dotenv()  # カレントディレクトリの .env ファイルを自動で読み込みます

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
    trashed=false を追加してゴミ箱内のフォルダを除外しています。
    """
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

    excel_files = list_excel_files_in_subfolders(
        shared_drive_id, target_folder_id
    )
    return excel_files


# 使用例：対象のExcelファイル一覧を取得
drive_file_list = list_excel_files_in_folder(SHARED_DRIVE_ID)

if not drive_file_list:
    print("対象フォルダ内にExcelファイルが見つかりませんでした。")
else:
    print("取得したファイル一覧:")
    for file in drive_file_list:
        print(f"タイトル: {file['title']}, ID: {file['id']}")


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


# ----------------------------------------------------------------
# 鹿内パート
# ----------------------------------------------------------------
def load_definition(file_path: str) -> pd.DataFrame:
    """
    財源定義エクセルファイルを読み込み、以下のカラムが含まれるデータフレームを返す。
      - 名前
      - 財源名/授業名
      - 雇用開始   (datetime 型に変換)
      - 雇用終了   (datetime 型に変換)
    """
    df_def = pd.read_excel(file_path)
    df_def["雇用開始"] = pd.to_datetime(df_def["雇用開始"], errors="coerce")
    df_def["雇用終了"] = pd.to_datetime(df_def["雇用終了"], errors="coerce")
    return df_def


def check_resources(
    df_standard: pd.DataFrame, df_def: pd.DataFrame, target_date: pd.Timestamp
) -> list[str]:
    error_messages = []

    # 対象年月に有効な定義のみを抽出
    df_def_valid = df_def[
        (df_def["雇用開始"] <= target_date)
        & (target_date <= df_def["雇用終了"])
    ]

    # 各人ごとに、対象年月に有効な財源のセットを作成
    valid_resources_by_name = {}
    for name, group in df_def_valid.groupby("名前"):
        valid_resources_by_name[name] = set(
            group["財源名/授業名"].dropna().astype(str)
        )

    # ＜チェック①＞　各人の全定義と、対象年月に有効な定義との差分＝更新推奨（【定義更新推奨】）
    for name, full_group in df_def.groupby("名前"):
        all_resources = set(full_group["財源名/授業名"].dropna().astype(str))
        valid_resources = valid_resources_by_name.get(name, set())
        rejected_resources = (
            all_resources - valid_resources
        )  # 対象年月に有効でない定義
        if rejected_resources:
            rejected_str = "\n".join(f"- {res}" for res in rejected_resources)
            error_messages.append(
                f"[定義更新推奨] {name} の以下の財源定義は対象年月 {target_date.strftime('%Y-%m')} には有効ではありません。財源定義を更新してください:\n{rejected_str}"
            )

    # ＜チェック②＞　対象年月に有効な定義で、df_standard に提出されているかチェック
    # 勤務記録が全くない場合も、提出数が少なければ未提出エラーとし、
    # また、提出されたPJコードの中に空文字があれば、PJコード未記入のエラーとして出力します。
    for name, valid_resources in valid_resources_by_name.items():
        # 該当人物の勤務記録（提出データ）を抽出
        df_std_name = df_standard[df_standard["name"] == name]
        # project_codeが空の場合は''で埋めた上で、リストにする
        submitted_resources = [
            x.replace("\u3000", "").strip()
            for x in df_std_name["project_code"].dropna().astype(str)
        ]

        # print(f"name: {name}")
        # print(f"valid_resources: {valid_resources}")
        # print(f"submitted_resources: {submitted_resources}")

        # PJコード未記入のエラー：空文字が含まれている場合
        blank_count = sum(1 for code in submitted_resources if code == "")
        if blank_count > 0:
            error_messages.append(
                f"[PJコード未記入] {name} の提出データに、PJコードが未記入の出勤簿が {blank_count} 件あります。"
            )

        # 未提出のチェック：提出リストの件数と有効定義の件数が異なる場合に判定
        if len(submitted_resources) != len(valid_resources):
            # 未提出分は、有効な定義から、非空の提出コード（実際に記入されたもの）を除いたものとする
            non_empty_submitted = [
                code for code in submitted_resources if code != ""
            ]
            missing_resources = valid_resources - set(non_empty_submitted)
            if missing_resources:
                missing_str = "\n".join(
                    f"- {res}" for res in missing_resources
                )
                error_messages.append(
                    f"[出勤簿未提出] {name} の提出データに、以下の出勤簿が不足しています:\n{missing_str}"
                )

    return error_messages


def get_definition_file(shared_drive_id: str) -> dict:
    """
    共有ドライブ内の「出勤簿」フォルダから「202503(test)」フォルダを取得し、
    その直下にある「財源定義.xlsx」ファイルの情報（dict）を返す。
    """
    # 「出勤簿」フォルダIDを取得
    shukkin_folder_id = get_folder_id_by_name(shared_drive_id, "出勤簿")

    # 「202503(test)」フォルダを取得
    query_folder = (
        "mimeType='application/vnd.google-apps.folder' and title='202503(test)' and "
        "'{}' in parents and trashed=false"
    ).format(shukkin_folder_id)
    folder_list = drive.ListFile(
        {
            "q": query_folder,
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

    # 「財源定義.xlsx」ファイルを、target_folder_id 内から取得
    query_file = (
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
        "title='財源定義.xlsx' and '{}' in parents and trashed=false"
    ).format(target_folder_id)
    file_list = drive.ListFile(
        {
            "q": query_file,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": shared_drive_id,
            "corpora": "drive",
        }
    ).GetList()
    if not file_list:
        raise Exception(
            "ファイル '財源定義.xlsx' が '202503(test)' 直下に見つかりませんでした。"
        )
    return file_list[0]


def load_definition_from_drive(
    shared_drive_id: str, download_dir: Path
) -> pd.DataFrame:
    """
    共有ドライブから「財源定義.xlsx」ファイルをダウンロードし、
    pandas.read_excel() で読み込んで df_def を作成して返す。
    """
    # ファイル情報を取得
    file_info = get_definition_file(shared_drive_id)
    # ローカル保存用パスを作成（DOWNLOAD_DIR は事前に作成してある前提）
    local_path = download_dir / file_info["title"]
    # ファイルをローカルにダウンロード
    file_info.GetContentFile(str(local_path))
    # Excelファイルを読み込み、雇用開始／雇用終了を datetime に変換
    df_def = pd.read_excel(local_path)
    df_def["雇用開始"] = pd.to_datetime(df_def["雇用開始"], errors="coerce")
    df_def["雇用終了"] = pd.to_datetime(df_def["雇用終了"], errors="coerce")
    return df_def


# -----------------------------------------------
# 鹿内パート終わり
# -----------------------------------------------


# -----------------------------------------------
# ⑤ メイン処理: Excel ファイルのチェック
# -----------------------------------------------

now = datetime.datetime.now()
target_date = pd.Timestamp(year=now.year, month=now.month, day=1)

# 財源定義ファイルの読み込み
df_def = load_definition_from_drive(SHARED_DRIVE_ID, DOWNLOAD_DIR)

df_standard = create_standard_dataframe(path_list)

error_messages = []

# 財源チェック (鹿内パート)
error_messages.extend(check_resources(df_standard, df_def, target_date))

# 森田パート
error_messages.extend(extract_errors_from_standard_df(df_standard))

# もともとの error メッセージを結合
error_message_formatted = "\n".join(error_messages)


# --- 以下、エラー行をファイル名の末尾部分（最後の'_'以降）ごとにグループ化する処理 ---
def extract_name_from_line(line: str) -> str:
    """
    各エラー行からファイル名部分を取り出し、拡張子などを除いた文字列の
    最後のアンダースコア '_' より後ろの部分を抽出する。

    例えば、
      "[勤務時間重複] 【3月勤務】AA出勤簿Ver.2.1(大関運営費_人A).xlsx - 2025-03-28 09:00:00 - 2025-03-28 12:00:00"
    の場合は、まずファイル名部分 "【3月勤務】AA出勤簿Ver.2.1(大関運営費_人A).xlsx" を取り出し、
    拡張子 ".xlsx" を除いて "【3月勤務】AA出勤簿Ver.2.1(大関運営費_人A)" とした上で、
    最後の '_' より後ろ、すなわち "人A" を返します。
    """
    try:
        # "]" の後ろにある文字列から "-" の前までをファイル名部分として取り出す
        file_name = line.split("]")[1].strip().split(" - ")[0]
    except IndexError:
        file_name = line
    # 拡張子除去
    base = file_name.rsplit(".", 1)[0]
    # 末尾の '_' 以降を抽出（存在しなければ "その他" とする）
    if "_" in base:
        extracted = base.rsplit("_", 1)[-1].strip("()")
        return extracted if extracted else "その他"
    return "その他"


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
        result_lines.append(f"■ {name} のエラー")
        for err_line in lines:
            result_lines.append("  " + err_line)
        result_lines.append("")  # グループ間の空行
    return "\n".join(result_lines)


# グループ化したエラーメッセージに置き換え
grouped_error_message = group_errors_by_name(error_message_formatted)

if grouped_error_message != "":
    send_slack_notification(
        "出勤簿に入力ミスがあります。\n" + grouped_error_message
    )
    print("grouped_error_message:", grouped_error_message)
    pass
else:
    print("全ての出勤簿に入力ミスはありませんでした。")
    send_slack_notification("Excel チェックは正常に終了しました。")


# -----------------------------------------------
# ⑥ 一時ディレクトリのクリーンアップ
# -----------------------------------------------
shutil.rmtree(DOWNLOAD_DIR)
