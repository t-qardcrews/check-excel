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
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)

# GoogleDrive のインスタンスを作成
drive = GoogleDrive(gauth)


# 一時的にダウンロードするディレクトリ
DOWNLOAD_DIR = Path("./temp_download")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# 共有ドライブのIDを環境変数から取得
SHARED_DRIVE_ID = os.environ.get("SHARED_DRIVE_ID")
if not SHARED_DRIVE_ID or SHARED_DRIVE_ID == "SHARED_DRIVE_ID":
    raise ValueError(
        "環境変数 SHARED_DRIVE_ID が正しく設定されていません。実際の共有ドライブのIDを設定してください。"
    )

# ================================================
# 【既存処理】出勤簿ファイルの取得用関数群
# ================================================

def get_folder_id_by_name(shared_drive_id: str, folder_name: str) -> str:
    query = "mimeType='application/vnd.google-apps.folder' and title='{}' and trashed=false".format(folder_name)
    folder_list = drive.ListFile({
        "q": query,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "driveId": shared_drive_id,
        "corpora": "drive",
    }).GetList()

    if not folder_list:
        raise Exception("フォルダ '{}' が共有ドライブ内に見つかりませんでした。".format(folder_name))
    return folder_list[0]["id"]


def list_excel_files_in_subfolders(shared_drive_id: str, parent_folder_id: str) -> list[dict]:
    subfolder_query = ("'{}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
                      ).format(parent_folder_id)
    subfolders = drive.ListFile({
        "q": subfolder_query,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "driveId": shared_drive_id,
        "corpora": "drive",
    }).GetList()

    excel_files = []
    for subfolder in subfolders:
        subfolder_id = subfolder["id"]
        file_query = (
            "'{}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
            "and title contains '【' and title contains '】' and trashed=false"
        ).format(subfolder_id)
        files = drive.ListFile({
            "q": file_query,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": shared_drive_id,
            "corpora": "drive",
        }).GetList()
        excel_files.extend(files)
    return excel_files


def list_excel_files_in_folder(shared_drive_id: str) -> list[dict]:
    shukkin_folder_id = get_folder_id_by_name(shared_drive_id, "出勤簿")
    query = ("mimeType='application/vnd.google-apps.folder' and title='202503(test)' and "
             "'{}' in parents and trashed=false").format(shukkin_folder_id)
    folder_list = drive.ListFile({
        "q": query,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "driveId": shared_drive_id,
        "corpora": "drive",
    }).GetList()

    if not folder_list:
        raise Exception("フォルダ '202503(test)' が '出勤簿' 内に見つかりませんでした。")
    target_folder_id = folder_list[0]["id"]

    excel_files = list_excel_files_in_subfolders(shared_drive_id, target_folder_id)
    return excel_files


# 使用例：出勤簿ファイル一覧を取得
drive_file_list = list_excel_files_in_folder(SHARED_DRIVE_ID)
if not drive_file_list:
    print("対象フォルダ内にExcelファイルが見つかりませんでした。")
else:
    print("取得した出勤簿ファイル一覧:")
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
print("\nダウンロードした出勤簿ファイルパス一覧:")
for path in path_list:
    print(path)


# ================================================
# 【追加】財源定義ファイルの取得・ダウンロード処理
# ================================================
def get_definition_file(shared_drive_id: str) -> dict:
    """
    出勤簿フォルダ内の「202503(test)」フォルダ直下から、
    タイトルに「財源定義」を含む Googleスプレッドシートファイルを取得する。
    """
    # まず「出勤簿」フォルダ内の「202503(test)」フォルダのIDを取得
    shukkin_folder_id = get_folder_id_by_name(shared_drive_id, "出勤簿")
    query = ("mimeType='application/vnd.google-apps.folder' and title='202503(test)' and "
             "'{}' in parents and trashed=false").format(shukkin_folder_id)
    folder_list = drive.ListFile({
        "q": query,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "driveId": shared_drive_id,
        "corpora": "drive",
    }).GetList()
    if not folder_list:
        raise Exception("フォルダ '202503(test)' が '出勤簿' 内に見つかりませんでした。")
    target_folder_id = folder_list[0]["id"]

    # 次に、「財源定義」という文字列を含むファイルを検索
    file_query = ("'{}' in parents and title contains '財源定義' and trashed=false"
                 ).format(target_folder_id)
    file_list = drive.ListFile({
        "q": file_query,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "driveId": shared_drive_id,
        "corpora": "drive",
    }).GetList()

    if not file_list:
        raise Exception("財源定義ファイルが見つかりませんでした。")
    return file_list[0]


def download_definition_file(definition_file: dict, download_dir: Path) -> Path:
    """
    GoogleスプレッドシートをExcel形式に変換してダウンロードする。
    保存名は "財源定義.xlsx" とする。
    """
    local_path = download_dir / "財源定義.xlsx"
    definition_file.GetContentFile(
        str(local_path),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    return local_path


# 財源定義ファイルの取得とダウンロード
try:
    definition_file = get_definition_file(SHARED_DRIVE_ID)
    definition_file_path = download_definition_file(definition_file, DOWNLOAD_DIR)
    print(f"\n財源定義ファイルをダウンロードしました: {definition_file_path}")
except Exception as e:
    print(f"\n財源定義ファイルの取得に失敗しました: {e}")
    definition_file_path = None


# ================================================
# 【既存処理】Excel ファイルの内容チェック用関数群
# ================================================
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

    df = pd.DataFrame({
        "start": date + start,
        "end": date + end,
        "remarks": remarks,
    })
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

    df_standard = df[[
        "name",
        "name_kana",
        "start",
        "end",
        "remarks",
        "project_code",
        "subject",
        "employment_type",
        "file_name",
    ]]
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
    error_message_set = set(error_message_list)
    return error_message_set


import pandas as pd
from collections import defaultdict

def check_ta_entries(df_standard: pd.DataFrame,
                     personal_data_df: pd.DataFrame,
                     definition_df: pd.DataFrame) -> list[str]:
    """
    TA チェックを行います。

    ・個人データシート（"個人データ"）からは各 TA の登録授業名（"財源名/授業名"）を取得し、
      財源定義シート（"財源定義"）からは、「雇用経費」が「運営費交付金」となっている行の
      "研究課題名（プロジェクトコード）" の値を取得します。

    ・各 TA について、個人データシートと財源定義シートから有効な授業名の共通部分を求め、
      出勤簿の subject と照合します。
      - subject が空欄の場合は [授業名不足] エラーを出力
      - subject が有効な授業名集合に含まれていなければエラーを出力
    ・また、TA の場合 project_code は空欄であるのが正しいので、project_code に値が入っているとエラーを出力します。
      ※ここでは、project_code が NaN または空文字の場合は正常とみなします。
    """
    error_messages = []

    # DataFrame の列名の余分な空白を除去する
    personal_data_df.columns = personal_data_df.columns.str.strip()
    definition_df.columns = definition_df.columns.str.strip()

    # ① 個人データシートから、各 TA の登録授業名集合を作成（キー：TAの名前、値：登録授業名の set）
    registered_subjects = defaultdict(set)
    for _, row in personal_data_df.iterrows():
        name = str(row["名前"]).strip()
        subject_value = str(row["財源名/授業名"]).strip()
        if subject_value:
            registered_subjects[name].add(subject_value)

    # ② 財源定義シートから、「雇用経費」が「運営費交付金」になっている行の
    #     "研究課題名（プロジェクトコード）" の値を集め、有効な TA 授業名の集合を作成
    valid_definition_subjects = set()
    for _, row in definition_df.iterrows():
        try:
            if str(row["雇用経費"]).strip() == "運営費交付金":
                subject_value = str(row["研究課題名（プロジェクトコード）"]).strip()
                if subject_value:
                    valid_definition_subjects.add(subject_value)
        except KeyError as e:
            print(f"定義シートに必要な列が存在しません: {e}")
            continue

    # ③ df_standard の TA 行に対してチェック
    ta_rows = df_standard[df_standard["employment_type"] == "TA"]
    for _, row in ta_rows.iterrows():
        file_name = row["file_name"]
        name = str(row["name"]).strip()
        subject = str(row["subject"]).strip()
        # project_code は数値型になっている可能性もあるので pd.isna() を用いてチェックする
        project_code = row["project_code"]

        # subject が空欄の場合は [授業名不足] エラー
        if subject == "":
            error_messages.append(
                f"[授業名不足] {file_name} - TAの授業名が記入されていません。"
            )
        else:
            # TA の有効な授業名は、個人データシートの登録授業名と財源定義シートの有効授業名の共通部分
            valid_subjects = registered_subjects.get(name, set()).intersection(valid_definition_subjects)
            if not valid_subjects:
                error_messages.append(
                    f"[TAエラー: 定義データ不一致] {file_name} - TAの名前 '{name}' に対して、"
                    f"個人データシートと財源定義シートの授業名が一致していません。"
                )
            else:
                # 入力された subject が、有効な授業名集合に含まれているかチェック
                if subject not in valid_subjects:
                    valid_list = ", ".join(valid_subjects)
                    error_messages.append(
                        f"[TAエラー: 授業名不一致] {file_name} - 記入された授業名 '{subject}' は、有効な授業名 ({valid_list}) と一致しません。"
                    )
        # project_code が NaN または空文字でなければエラー（TA の場合は空欄が正しい）
        if not pd.isna(project_code) and str(project_code).strip() != "":
            error_messages.append(
                f"[TAエラー: PJコード非空欄ミス] {file_name} - TAの出勤簿ではプロジェクトコードは空欄にしてください。"
            )
    
    return error_messages




# ================================================
# 【既存処理】Slack 通知用の関数
# ================================================
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")

def send_slack_notification(message: str):
    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK, json=payload)
    if response.status_code != 200:
        print("Slack 通知に失敗しました:", response.text)


# ================================================
# 【メイン処理】Excel ファイルのチェック
# ================================================
df_standard = create_standard_dataframe(path_list)
error_message_set = extract_errors_from_standard_df(df_standard)
error_message_formatted = "\n".join(error_message_set)

# 財源定義ファイルが存在する場合
if definition_file_path is not None:
    # 個人データシートの読み込み（シート名「個人データ」）
    personal_data_df = pd.read_excel(definition_file_path, sheet_name="個人データ")
    # 財源定義シートの読み込み（シート名「財源定義」）
    definition_df = pd.read_excel(definition_file_path, sheet_name="財源定義")
    
    # df_standard（出勤簿の DataFrame）に対して TA チェックを実施
    ta_error_messages = check_ta_entries(df_standard, personal_data_df, definition_df)
else:
    print("財源定義ファイルがなかったため、TAチェックはスキップされます。")
    ta_error_messages = []



all_error_messages = set(error_message_set).union(set(ta_error_messages))

def extract_name_from_line(line: str) -> str:
    try:
        file_name = line.split("]")[1].strip().split(" - ")[0]
    except IndexError:
        file_name = line
    base = file_name.rsplit(".", 1)[0]
    if "_" in base:
        extracted = base.rsplit("_", 1)[-1].strip("()")
        return extracted if extracted else "その他"
    return "その他"


def group_errors_by_name(error_message_formatted: str) -> str:
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
        result_lines.append("")
    return "\n".join(result_lines)


grouped_error_message = group_errors_by_name("\n".join(all_error_messages))

if grouped_error_message != "":
    send_slack_notification("出勤簿に入力ミスがあります。\n" + grouped_error_message)
else:
    print("全ての出勤簿に入力ミスはありませんでした。")
    send_slack_notification("Excel チェックは正常に終了しました。")


# ================================================
# 一時ディレクトリのクリーンアップ
# ================================================
shutil.rmtree(DOWNLOAD_DIR)
