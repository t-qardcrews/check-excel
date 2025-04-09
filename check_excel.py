import datetime
import json
import os
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Optional, Union, Dict, List

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# =============================================================================
# 定数・環境設定
# =============================================================================
MESSAGE_HEADER = "【ここにメッセージヘッダーを書く】\n"

# .env ファイルから環境変数を読み込み
load_dotenv()

# Google Drive 認証設定
service_account_info = json.loads(os.environ["GDRIVE_CREDENTIALS"])
scope = ["https://www.googleapis.com/auth/drive"]
gauth = GoogleAuth()
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
drive = GoogleDrive(gauth)

SHARED_DRIVE_ID = os.environ.get("SHARED_DRIVE_ID")
if not SHARED_DRIVE_ID or SHARED_DRIVE_ID == "SHARED_DRIVE_ID":
    raise ValueError("環境変数 SHARED_DRIVE_ID が正しく設定されていません。")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")

# 一時ダウンロードフォルダ
DOWNLOAD_DIR = Path("./temp_download")
DOWNLOAD_DIR.mkdir(exist_ok=True, parents=True)


# =============================================================================
# DriveDownloader クラス
# =============================================================================
class DriveDownloader:
    """
    Google Drive 上から指定フォルダ内のファイル情報取得、フォルダ構造を再現したダウンロード、
    およびダウンロード済みファイルの読み込みを行うクラス。
    """

    def __init__(self, drive: GoogleDrive, shared_drive_id: str, download_root: Union[str, Path]):
        self.drive = drive
        self.shared_drive_id = shared_drive_id
        self.download_root = Path(download_root) if not isinstance(download_root, Path) else download_root

    def get_folder_id(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """
        共有ドライブ内から指定フォルダ名のフォルダ ID を取得する。

        Parameters:
            folder_name: 探索するフォルダ名
            parent_folder_id: 親フォルダの ID (指定時はその直下のみ検索)

        Returns:
            フォルダ ID (文字列)

        Raises:
            FileNotFoundError: フォルダが見つからなかった場合
        """
        if parent_folder_id:
            query = (
                f"mimeType='application/vnd.google-apps.folder' and title='{folder_name}' "
                f"and '{parent_folder_id}' in parents and trashed=false"
            )
        else:
            query = (
                f"mimeType='application/vnd.google-apps.folder' and title='{folder_name}' "
                "and trashed=false"
            )
        folder_list = self.drive.ListFile({
            "q": query,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": self.shared_drive_id,
            "corpora": "drive",
        }).GetList()
        if not folder_list:
            raise FileNotFoundError(f"フォルダ '{folder_name}' が見つかりませんでした。")
        return folder_list[0]["id"]

    def gather_file_info(self, parent_folder_name: str = "出勤簿", target_subfolder_name: str = "202503(test)") -> Dict[str, Union[Dict, List[Dict]]]:
        """
        「出勤簿」フォルダ→「202503(test)」フォルダ内のファイル情報を取得する。

        Returns:
            {
                "definition_file": 財源定義.xlsx のファイル情報 (dict または None),
                "timesheet_files": 出勤簿 XLSX ファイル情報 (dict のリスト)
            }
        """
        parent_folder_id = self.get_folder_id(parent_folder_name)
        target_folder_id = self.get_folder_id(target_subfolder_name, parent_folder_id=parent_folder_id)

        # 財源定義.xlsx の検索
        definition_query = (
            "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
            "and title='財源定義.xlsx' "
            f"and '{target_folder_id}' in parents and trashed=false"
        )
        definition_files = self.drive.ListFile({
            "q": definition_query,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": self.shared_drive_id,
            "corpora": "drive",
        }).GetList()
        definition_file = definition_files[0] if definition_files else None

        # 出勤簿ファイルの検索：target_folder_id 配下のサブフォルダからタイトルに「【」と「】」を含む XLSX を抽出
        subfolder_query = (
            f"'{target_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        subfolders = self.drive.ListFile({
            "q": subfolder_query,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "driveId": self.shared_drive_id,
            "corpora": "drive",
        }).GetList()
        timesheet_files = []
        for sf in subfolders:
            sf_id = sf["id"]
            file_query = (
                f"'{sf_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
                "and title contains '【' and title contains '】' and trashed=false"
            )
            files = self.drive.ListFile({
                "q": file_query,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
                "driveId": self.shared_drive_id,
                "corpora": "drive",
            }).GetList()
            timesheet_files.extend(files)
        return {"definition_file": definition_file, "timesheet_files": timesheet_files}

    def _build_local_subfolder_path(self, folder_id: str) -> Path:
        """
        Drive 上のフォルダ構造を再帰的に辿り、ローカルパスを構築する内部関数。

        Parameters:
            folder_id: Drive 上のフォルダ ID

        Returns:
            再現したローカルパス (Path)
        """
        file_obj = self.drive.CreateFile({"id": folder_id, "supportsAllDrives": True})
        file_obj.FetchMetadata(fields="title,parents")
        folder_title = file_obj["title"]
        local_subfolder = Path(folder_title)
        parent_list = file_obj.get("parents", [])
        if not parent_list:
            return self.download_root / local_subfolder
        parent_id = None
        for p in parent_list:
            if not p.get("isRoot", False) and not p.get("trashed", True):
                parent_id = p["id"]
                break
        if not parent_id:
            return self.download_root / local_subfolder
        parent_path = self._build_local_subfolder_path(parent_id)
        return parent_path / local_subfolder

    def download_files(self, file_info_list: List[Dict]) -> None:
        """
        渡されたファイル情報リストに従い、Drive 上のフォルダ構造を再現したうえで
        download_root 配下へファイルをダウンロードする。

        Parameters:
            file_info_list: ファイル情報 (dict) のリスト
        """
        self.download_root.mkdir(parents=True, exist_ok=True)
        for file_dict in file_info_list:
            parent_ids = file_dict.get("parents", [])
            if not parent_ids:
                parent_local_dir = self.download_root
            else:
                parent_id = parent_ids[0]["id"] if isinstance(parent_ids[0], dict) else parent_ids[0]
                parent_local_dir = self._build_local_subfolder_path(parent_id)
            parent_local_dir.mkdir(parents=True, exist_ok=True)
            local_file_path = parent_local_dir / file_dict["title"]
            file_dict.GetContentFile(str(local_file_path),
                                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            print(f"Downloaded: {local_file_path}")

    def load_xlsx_data(self) -> Dict[str, pd.DataFrame]:
        """
        ダウンロード先フォルダ（サブフォルダ含む）内の全 XLSX ファイルを読み込み、
        download_root からの相対パスをキー、DataFrame を値として返す。

        Returns:
            { 'relative/path/to/file.xlsx': DataFrame, ... }
        """
        result = {}
        for xlsx_path in self.download_root.rglob("*.xlsx"):
            df = pd.read_excel(xlsx_path)
            rel_path = str(xlsx_path.relative_to(self.download_root))
            result[rel_path] = df
        return result


# =============================================================================
# StandardDataFrameBuilder クラス
# =============================================================================
class StandardDataFrameBuilder:
    """
    出勤簿 XLSX ファイルから個人情報・勤務データを抽出し、
    標準形式の DataFrame (df_standard) を作成するクラス。
    """

    @staticmethod
    def extract_name(df_raw: pd.DataFrame) -> tuple[str, str]:
        name = str(df_raw[2].iat[2])
        name_kana = str(df_raw[2].iat[1])
        return "".join(name.split()), "".join(name_kana.split())

    @staticmethod
    def extract_date(df_raw: pd.DataFrame) -> pd.DataFrame:
        date_arr = np.concatenate([df_raw[0][5:37].values, df_raw[7][5:35].values])
        date_series = pd.Series(date_arr)
        start_series = pd.concat([df_raw[2][5:37], df_raw[9][5:35]], ignore_index=True)
        end_series = pd.concat([df_raw[4][5:37], df_raw[11][5:35]], ignore_index=True)
        remarks_series = pd.concat([df_raw[6][5:37], df_raw[13][5:35]], ignore_index=True).fillna("")

        date_series.iloc[1::2] = date_series.iloc[0::2]
        remarks_series.iloc[1::2] = remarks_series.iloc[0::2]

        date_series = pd.to_datetime(date_series, errors="coerce")
        start_series = pd.to_timedelta(start_series, errors="coerce")
        end_series = pd.to_timedelta(end_series, errors="coerce")

        df = pd.DataFrame({
            "start": date_series + start_series,
            "end": date_series + end_series,
            "remarks": remarks_series,
        })
        df = df.dropna(subset=["start", "end"], how="all").reset_index(drop=True)
        return df

    @staticmethod
    def extract_project_code(df_raw: pd.DataFrame) -> str:
        project_code = df_raw[1].iat[42]
        if pd.isna(project_code):
            return ""
        return "".join(str(project_code).split())

    @staticmethod
    def extract_employment_type(df_raw: pd.DataFrame) -> str:
        employment_type = df_raw[0].iat[0]
        if employment_type == "アドミニストレイティブ・アシスタント出勤簿":
            return "AA"
        elif employment_type == "ティーチング・アシスタント出勤簿":
            return "TA"
        elif employment_type == "リサーチ・アシスタント出勤簿":
            return "RA"
        return ""

    @staticmethod
    def extract_subject(df_raw: pd.DataFrame) -> str:
        subject = df_raw[1].iat[44]
        if pd.isna(subject):
            return ""
        return "".join(str(subject).split())

    @staticmethod
    def create_standard_dataframe_single(path: Path) -> pd.DataFrame:
        df_raw = pd.read_excel(path, sheet_name="出勤簿様式", header=None)
        name, name_kana = StandardDataFrameBuilder.extract_name(df_raw)
        date_df = StandardDataFrameBuilder.extract_date(df_raw)
        project_code = StandardDataFrameBuilder.extract_project_code(df_raw)
        employment_type = StandardDataFrameBuilder.extract_employment_type(df_raw)
        subject = StandardDataFrameBuilder.extract_subject(df_raw)

        df = date_df.copy()
        df["name"] = name
        df["name_kana"] = name_kana
        df["project_code"] = project_code
        df["employment_type"] = employment_type
        df["subject"] = subject
        df["file_name"] = path.name

        columns = ["name", "name_kana", "start", "end", "remarks",
                   "project_code", "subject", "employment_type", "file_name"]
        return df[columns]

    @staticmethod
    def sort_df_standard(df_standard: pd.DataFrame) -> pd.DataFrame:
        df_standard.sort_values("start", inplace=True)
        df_standard.sort_values("name_kana", inplace=True)
        df_standard.reset_index(drop=True, inplace=True)
        return df_standard

    @staticmethod
    def create_standard_dataframe(path_list: List[Path]) -> pd.DataFrame:
        df_list = []
        for path in path_list:
            if not isinstance(path, Path):
                path = Path(path)
            df_list.append(StandardDataFrameBuilder.create_standard_dataframe_single(path))
        df_standard = pd.concat(df_list, ignore_index=True)
        return StandardDataFrameBuilder.sort_df_standard(df_standard)


# =============================================================================
# TimesheetChecker クラス
# =============================================================================
class TimesheetChecker:
    """
    df_standard に対して勤務時間の各チェック（重複、連続日数、6時間累計、週合計、時間帯）を実施するクラス。
    """

    def __init__(self, df_standard: pd.DataFrame):
        self.df_standard = df_standard.copy()
        self.df_standard["name_and_name_kana"] = self.df_standard["name"] + "-" + self.df_standard["name_kana"]

    def check_overlaps(self, group: pd.DataFrame) -> List[str]:
        messages = []
        grp = group.sort_values("start")
        for i in range(len(grp) - 1):
            current = grp.iloc[i]
            next_row = grp.iloc[i + 1]
            if current["end"] >= next_row["start"]:
                messages.append(f"[勤務時間重複] {current['start'].date()} - {current['file_name']}")
                messages.append(f"[勤務時間重複] {next_row['start'].date()} - {next_row['file_name']}")
        return messages

    def check_consecutive_days(self, group: pd.DataFrame) -> List[str]:
        errors = []
        if group.empty:
            return errors
        grp = group.sort_values("start").copy()
        grp["work_date"] = grp["start"].dt.date
        unique_dates = sorted(grp["work_date"].unique())
        continuous_blocks = []
        if unique_dates:
            block_dates = [unique_dates[0]]
            prev_date = unique_dates[0]
            for current_date in unique_dates[1:]:
                if (current_date - prev_date).days == 1:
                    block_dates.append(current_date)
                else:
                    if len(block_dates) > 5:
                        continuous_blocks.append(set(block_dates))
                    block_dates = [current_date]
                prev_date = current_date
            if len(block_dates) > 5:
                continuous_blocks.append(set(block_dates))
        for _, row in grp.iterrows():
            for block in continuous_blocks:
                if row["work_date"] in block:
                    errors.append(f"[連続5日超過] {row['start'].date()} - {row['file_name']}")
                    break
        return errors

    def check_consecutive_hours(self, group: pd.DataFrame) -> List[str]:
        errors = []
        if group.empty:
            return errors
        grp = group.sort_values("start").copy()
        grp["cumulative_hours"] = 0
        for i in range(len(grp)):
            current_start = grp.iloc[i]["start"]
            window = grp[(grp["start"] >= current_start) & (grp["start"] < current_start + pd.Timedelta(hours=6))]
            total = (window["end"] - window["start"]).sum()
            grp.at[grp.index[i], "cumulative_hours"] = total.total_seconds() / 3600
        for _, row in grp.iterrows():
            if row["cumulative_hours"] > 6:
                errors.append(f"[連続6時間超過] {row['start'].date()} - {row['file_name']}")
        return errors

    def check_weekly_hours(self, group: pd.DataFrame) -> List[str]:
        errors = []
        if group.empty:
            return errors
        grp = group.copy()
        grp["week"] = grp["start"].dt.to_period("W")
        weekly_hours = grp.groupby("week").apply(lambda x: (x["end"] - x["start"]).sum().total_seconds() / 3600)
        for week, hours in weekly_hours.items():
            if hours > 28:
                for _, row in grp[grp["week"] == week].iterrows():
                    errors.append(f"[週28時間超過] {row['start'].date()} - {row['file_name']}")
        return errors

    def check_allowed_time(self, group: pd.DataFrame) -> List[str]:
        errors = []
        allowed_start = pd.Timestamp("1900-01-01 08:30:00").time()
        allowed_end = pd.Timestamp("1900-01-01 17:15:00").time()
        for _, row in group.iterrows():
            st = row["start"].time()
            ed = row["end"].time()
            if st < allowed_start or ed > allowed_end:
                errors.append(f"[時間外勤務] {row['start'].date()} - {row['file_name']}")
        return errors

    def run_all_checks(self) -> List[str]:
        errors = []
        for _, group in self.df_standard.groupby("name_and_name_kana"):
            errors.extend(self.check_overlaps(group))
            errors.extend(self.check_consecutive_days(group))
            errors.extend(self.check_consecutive_hours(group))
            errors.extend(self.check_weekly_hours(group))
            errors.extend(self.check_allowed_time(group))
        return errors


# =============================================================================
# ResourceChecker クラス
# =============================================================================
class ResourceChecker:
    """
    財源定義（df_def）と出勤簿の提出データ（df_standard）を元に、定義の更新推奨、
    PJコード未記入、および出勤簿未提出の各チェックを実施するクラス。
    """

    def __init__(self, df_standard: pd.DataFrame, df_def: pd.DataFrame, target_date: pd.Timestamp):
        self.df_standard = df_standard.copy()
        self.df_def = df_def.copy()
        self.target_date = target_date

    @staticmethod
    def extract_active_definitions_by_employee(df_def: pd.DataFrame, target_date: pd.Timestamp) -> Dict[str, set]:
        df_valid = df_def[(df_def["雇用開始"] <= target_date) & (target_date <= df_def["雇用終了"])]
        return {name: set(group["財源名/授業名"].dropna().astype(str))
                for name, group in df_valid.groupby("名前")}

    @staticmethod
    def check_definitions_outdated(df_def: pd.DataFrame, active_defs: Dict[str, set], target_date: pd.Timestamp) -> List[str]:
        errors = []
        for name, group in df_def.groupby("名前"):
            all_defs = set(group["財源名/授業名"].dropna().astype(str))
            valid_defs = active_defs.get(name, set())
            outdated = all_defs - valid_defs
            if outdated:
                outdated_str = "\n".join(f"- {d}" for d in outdated)
                errors.append(f"[定義更新推奨] {name} の以下の財源定義は対象年月 {target_date.strftime('%Y-%m')} には有効ではありません。更新してください:\n{outdated_str}")
        return errors

    @staticmethod
    def check_pj_code_not_filled(df_standard: pd.DataFrame, active_defs: Dict[str, set]) -> List[str]:
        errors = []
        names = df_standard["name"].unique()
        for name in names:
            df_name = df_standard[df_standard["name"] == name]
            for _, row in df_name.iterrows():
                if row["project_code"] == "" and row["employment_type"] != "TA":
                    errors.append(f"[PJコード未記入] PJコード未記入 - {row['file_name']}")
        return errors

    @staticmethod
    def check_assigned_but_not_submitted(df_standard: pd.DataFrame, active_defs: Dict[str, set]) -> List[str]:
        errors = []
        for name, valid_defs in active_defs.items():
            df_name = df_standard[df_standard["name"] == name]
            submitted = [code.replace("\u3000", "").strip() for code in df_name["project_code"].dropna().astype(str)]
            if len(submitted) != len(valid_defs):
                non_empty = [code for code in submitted if code != ""]
                missing = valid_defs - set(non_empty)
                if missing:
                    missing_str = "\n".join(f"- {d}" for d in missing)
                    errors.append(f"[出勤簿未提出] {name} の提出データに不足:\n{missing_str}")
        return errors

    def run_resource_checks(self) -> List[str]:
        errors = []
        active_defs = self.extract_active_definitions_by_employee(self.df_def, self.target_date)
        errors.extend(self.check_definitions_outdated(self.df_def, active_defs, self.target_date))
        errors.extend(self.check_pj_code_not_filled(self.df_standard, active_defs))
        errors.extend(self.check_assigned_but_not_submitted(self.df_standard, active_defs))
        return errors


# =============================================================================
# ErrorGrouper クラス
# =============================================================================
class ErrorGrouper:
    """
    エラーメッセージの各行から、ファイル名末尾の識別情報（従業員名推定）を抽出し、
    グループ化してレポート文字列を作成するクラス。
    """

    @staticmethod
    def extract_name_from_line(line: str) -> str:
        try:
            file_name = line.split("]")[1].strip().split(" - ")[-1]
        except IndexError:
            file_name = line
        base = file_name.rsplit(".", 1)[0]
        if "_" in base:
            extracted = base.rsplit("_", 1)[-1].strip("()")
            return extracted if extracted else "その他"
        return "その他"

    @staticmethod
    def group_errors_by_name(error_message: str) -> str:
        lines = error_message.splitlines()
        groups = defaultdict(list)
        for line in lines:
            name = ErrorGrouper.extract_name_from_line(line)
            groups[name].append(line)
        result_lines = []
        for name, errs in groups.items():
            if name == "その他":
                continue
            errs.sort()
            result_lines.append(f"■ {name} のエラー")
            for err in errs:
                result_lines.append(f"  {err}")
            result_lines.append("")
        if "その他" in groups:
            result_lines.append("■ その他のエラー")
            for err in groups["その他"]:
                result_lines.append(f"  {err}")
        return "\n".join(result_lines)


# =============================================================================
# ResourceDefinitionLoader クラス
# =============================================================================
class ResourceDefinitionLoader:
    """
    財源定義エクセルファイル（財源定義.xlsx）を読み込み、雇用開始／終了日を datetime 型に変換した DataFrame を返すクラス。
    """

    @staticmethod
    def load_definition_from_file(file_path: Union[str, Path]) -> pd.DataFrame:
        df_def = pd.read_excel(file_path)
        df_def["雇用開始"] = pd.to_datetime(df_def["雇用開始"], errors="coerce")
        df_def["雇用終了"] = pd.to_datetime(df_def["雇用終了"], errors="coerce")
        return df_def


# =============================================================================
# Slack 通知
# =============================================================================
def send_slack_notification(message: str) -> None:
    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK, json=payload)
    if response.status_code != 200:
        print(f"Slack 通知に失敗しました: {response.text}")


# =============================================================================
# main 関数
# =============================================================================
def main() -> None:
    """
    全体の処理フロー:
      1. Google Drive から出勤簿関連ファイル（財源定義.xlsx および出勤簿 XLSX ファイル）の情報取得
      2. フォルダ構造を再現して一時フォルダへダウンロード
      3. ダウンロード済み XLSX ファイルから標準 DataFrame (df_standard) の作成
      4. 財源定義ファイルを読み込み (df_def)
      5. 出勤簿チェック（勤務時間チェックおよび財源定義チェック）を実施
      6. エラーレポートをグループ化し Slack へ通知
      7. 一時フォルダを削除
    """
    print("=== Google Drive からデータを取得 ===")
    downloader = DriveDownloader(drive, SHARED_DRIVE_ID, download_root=DOWNLOAD_DIR)
    file_info_dict = downloader.gather_file_info(parent_folder_name="出勤簿", target_subfolder_name="202503(test)")

    definition_file = file_info_dict.get("definition_file")
    timesheet_files = file_info_dict.get("timesheet_files", [])

    if definition_file:
        print(f"財源定義ファイル: {definition_file['title']}")
    else:
        print("財源定義ファイルが見つかりませんでした。")
    if timesheet_files:
        print("出勤簿ファイル一覧:")
        for f in timesheet_files:
            print(f"  - {f['title']}")
    else:
        print("出勤簿ファイルが見つかりませんでした。")

    # ダウンロード対象ファイルリストを作成
    files_to_download = []
    if definition_file:
        files_to_download.append(definition_file)
    files_to_download.extend(timesheet_files)

    # 2. ファイルダウンロード
    downloader.download_files(files_to_download)

    # 3. ダウンロード済みファイルから XLSX データの読み込み
    xlsx_data = downloader.load_xlsx_data()
    print("\n=== 読み込んだ XLSX データ ===")
    for rel_path, df in xlsx_data.items():
        print(f"--- {rel_path} ---")
        print(df.head())

    # 4. 標準出勤簿 DataFrame (df_standard) の作成（財源定義.xlsx を除く）
    standard_paths = []
    for rel_path in xlsx_data.keys():
        if "財源定義.xlsx" not in rel_path:
            standard_paths.append(downloader.download_root / rel_path)
    if not standard_paths:
        print("出勤簿データが存在しません。")
        return
    df_standard = StandardDataFrameBuilder.create_standard_dataframe(standard_paths)
    print("\n=== 作成された標準 DataFrame ===")
    print(df_standard.head())

    # 5. 財源定義ファイルの読み込み（ある場合）
    if definition_file:
        # ダウンロード済みのファイルからキーに「財源定義.xlsx」が含まれるものを抽出
        def_path = None
        for key in xlsx_data.keys():
            if "財源定義.xlsx" in key:
                def_path = downloader.download_root / key
                break
        if def_path is None:
            print("財源定義ファイルが読み込めませんでした。")
            return
        df_def = ResourceDefinitionLoader.load_definition_from_file(def_path)
    else:
        print("財源定義ファイルが存在しないため、リソースチェックはスキップします。")
        df_def = pd.DataFrame()

    # 6. 勤務時間チェック実施
    ts_checker = TimesheetChecker(df_standard)
    working_errors = ts_checker.run_all_checks()

    # 7. 財源定義チェック（pjコード、未提出定義など）実施（df_def が存在する場合）
    resource_errors = []
    if not df_def.empty:
        rc = ResourceChecker(df_standard, df_def, pd.Timestamp(datetime.datetime.now().year,
                                                                 datetime.datetime.now().month, 1))
        resource_errors = rc.run_resource_checks()

    all_errors = set(working_errors + resource_errors)
    error_message = "\n".join(all_errors)
    grouped_message = ErrorGrouper.group_errors_by_name(error_message) if error_message else ""
    final_message = MESSAGE_HEADER + "\n" + (grouped_message if grouped_message else "Excel チェックは正常に終了しました。")
    print("\n=== エラーレポート ===")
    print(final_message)

    # send_slack_notification(final_message)

    # 8. 一時フォルダ削除
    shutil.rmtree(downloader.download_root)
    print("一時フォルダを削除しました。")


if __name__ == "__main__":
    main()
