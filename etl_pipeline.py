import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from pathlib import Path

# ----------------------
# 環境變數與全域設定
# ----------------------

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True, encoding="utf-8") or load_dotenv(find_dotenv(), override=True, encoding="utf-8")

# 修正可能存在的 BOM 鍵名
for k in list(os.environ.keys()):
    if k.startswith("\ufeff"):
        os.environ[k.lstrip("\ufeff")] = os.environ.pop(k)

DB_URL = os.getenv("DB_URL", "").strip()
if not DB_URL:
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db   = os.getenv("PG_DB", "postgres")
    pg_user = os.getenv("PG_USER", "postgres")
    pg_pwd  = os.getenv("PG_PASSWORD", "")
    DB_URL = f"postgresql+psycopg2://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}"


EXCEL_PATH = os.getenv(
    "EXCEL_PATH",
    "C:/Users/admin/Desktop/info/side_project/English_retail/data/online_retail_II.xlsx",
).strip()
SHEETS_ENV = os.getenv("SHEETS", "Year 2009-2010,Year 2010-2011").strip()

DAILY_REVENUE_MIN = float(os.getenv("DAILY_REVENUE_MIN", "1000"))
DAILY_REVENUE_MAX = float(os.getenv("DAILY_REVENUE_MAX", "5000000"))
MISSING_CUST_MAX = float(os.getenv("MISSING_CUSTOMER_ID_MAX_RATIO", "0.1"))

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "").strip()
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "").strip()
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()

SCHEMA_NAME = os.getenv("SCHEMA_NAME", "etl").strip()

def _engine():
    """建立 SQLAlchemy engine（連線錯誤給出清楚提示）"""
    if not DB_URL:
        raise RuntimeError(
            "DB_URL 未設定。請在 .env 或系統環境變數中設定，例如：\n"
            "DB_URL=postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/mydb"
        )
    try:
        eng = create_engine(DB_URL, pool_pre_ping=True)
        # 簡單測試連線
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except SQLAlchemyError as e:
        raise RuntimeError(f"無法連線到資料庫，請檢查 DB_URL。詳情：{e}")


# ----------------------
# E (Extract)
# ----------------------
def extract_data() -> pd.DataFrame:
    """
    讀取 Excel -> 回傳合併後的 DataFrame
    - SHEETS 指定要讀的工作表，逗號分隔；若為空字串，則讀全部 sheet
    """
    excel_path = Path(EXCEL_PATH)
    if not excel_path.exists():
        raise FileNotFoundError(
            f"找不到 Excel 檔案：{excel_path}\n"
            "請確認 EXCEL_PATH 是否正確，或將檔案放在該路徑。"
        )

    print(f"📥 讀取 Excel：{excel_path}")
    # 解析 SHEETS（允許空，空=全讀）
    sheets = [s.strip() for s in SHEETS_ENV.split(",") if s.strip()]
    # 讀取
    if sheets:
        frames = []
        for s in sheets:
            print(f"  - 讀取 sheet: {s}")
            df = pd.read_excel(excel_path, sheet_name=s, engine="openpyxl")
            df["source_sheet"] = s
            frames.append(df)
        raw = pd.concat(frames, ignore_index=True)
    else:
        # 全部 sheet
        xls = pd.ExcelFile(excel_path, engine="openpyxl")
        frames = []
        for s in xls.sheet_names:
            print(f"  - 讀取 sheet: {s}")
            df = pd.read_excel(xls, sheet_name=s)
            df["source_sheet"] = s
            frames.append(df)
        raw = pd.concat(frames, ignore_index=True)

    print(f"📦 擷取完成：{len(raw):,} 列")
    return raw


# ----------------------
# T (Transform)
# ----------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    欄位標準化 + 型別轉換 + 計算 total_amount + 退貨辨識 + year_month
    """
    print("🧪 進行轉換與清理 ...")
    # 來源資料不一致時的一些常見欄位名對應
    rename_candidates = {
        "Invoice": "invoice",
        "InvoiceNo": "invoice",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "Price": "unit_price",
        "UnitPrice": "unit_price",
        "Customer ID": "customer_id",
        "CustomerID": "customer_id",
        "Country": "country",
    }
    # 寬鬆比對（去首尾空白）
    df = df.rename(columns={k: v for k, v in rename_candidates.items() if k in df.columns})

    # 確保必要欄位存在
    required = ["invoice", "stock_code", "description", "quantity", "invoice_date", "unit_price", "country"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"缺少必要欄位：{missing}\n請確認原始檔的欄位名稱與 SHEETS 是否正確。")

    # 型別處理
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].astype("string")
    else:
        df["customer_id"] = pd.Series(pd.NA, dtype="string")

    df["stock_code"] = df["stock_code"].astype("string")
    df["description"] = df["description"].astype("string").str.strip()

    # 計算金額
    df["total_amount"] = df["quantity"].fillna(0) * df["unit_price"].fillna(0)

    # 退貨辨識：Invoice 以 C 開頭 或 數量 < 0
    df["is_return"] = df["invoice"].astype(str).str.startswith("C") | (df["quantity"] < 0)

    # 年月（字串）
    df["year_month"] = df["invoice_date"].dt.to_period("M").astype(str)

    # 基本行數摘要
    n_null_date = int(df["invoice_date"].isna().sum())
    n_returns = int(df["is_return"].sum())
    print(f"🧮 轉換完成：{len(df):,} 列｜空日期 {n_null_date:,} 列｜退貨 {n_returns:,} 列")
    return df


# ----------------------
# L (Load)
# ----------------------
def load_data(df: pd.DataFrame):
    """寫進 PostgreSQL：staging + cleaned（排除退貨、負數/單價<=0）"""
    print("💾 載入資料到 PostgreSQL ...")
    eng = _engine()
    with eng.begin() as conn:
        df.to_sql("sales_staging", conn, if_exists="replace", index=False, schema=SCHEMA_NAME)

        cleaned = df.loc[
            (~df["is_return"]) &
            (df["quantity"].fillna(0) >= 0) &
            (df["unit_price"].fillna(0) > 0)
        ].copy()

        cleaned.to_sql("sales_cleaned", conn, if_exists="replace", index=False, schema=SCHEMA_NAME)
        print(f"✅ 寫入完成：{SCHEMA_NAME}.sales_staging={len(df):,} 列, {SCHEMA_NAME}.sales_cleaned={len(cleaned):,} 列")


# ----------------------
# 通知（選擇性執行）
# ----------------------
def _send_slack(msg: str):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        import requests  # 輕量相依，未設定就不發
        requests.post(SLACK_WEBHOOK_URL, json={"text": msg}, timeout=10)
    except Exception as e:
        print(f"Slack send failed: {e}")


def _send_email(msg: str):
    if not (ALERT_EMAIL_FROM and ALERT_EMAIL_TO and SMTP_HOST and SMTP_USER and SMTP_PASS):
        return
    try:
        import smtplib
        from email.mime.text import MIMEText

        m = MIMEText(msg, "plain", "utf-8")
        m["Subject"] = "ETL Data Quality Alert"
        m["From"] = ALERT_EMAIL_FROM
        m["To"] = ALERT_EMAIL_TO
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(m)
    except Exception as e:
        print(f"Email send failed: {e}")


# ----------------------
# DQ（每日監控）
# ----------------------
def quality_check():
    """
    每日 DQ：當日銷售總額、缺失 customer_id 比例；異常則警示並記錄到 dq_monitor_log
    """
    print("🔎 執行每日 DQ 檢查 ...")
    eng = _engine()
    with eng.begin() as conn:
        # 若表不存在，直接給提示並返回（避免第一次就報錯）
        exists = conn.execute(
            text(
                """
                SELECT to_regclass('public.sales_cleaned') IS NOT NULL AS exists_cleaned
                """
            )
        ).scalar()
        if not exists:
            print("ℹ️ 尚未找到表 'sales_cleaned'，請先執行 ETL 載入。略過 DQ。")
            return

        q = """
            SELECT
                COALESCE(SUM(total_amount), 0) AS daily_total,
                AVG(CASE WHEN customer_id IS NULL OR customer_id = '' THEN 1.0 ELSE 0.0 END) AS missing_ratio
            FROM sales_cleaned
            WHERE invoice_date::date = CURRENT_DATE
        """
        row = conn.execute(text(q)).mappings().first() or {}
        daily_total = float(row.get("daily_total") or 0.0)
        missing_ratio = float(row.get("missing_ratio") or 0.0)

        status = "PASS"
        alerts = []
        if not (DAILY_REVENUE_MIN <= daily_total <= DAILY_REVENUE_MAX):
            alerts.append(
                f"daily_total={daily_total:,.2f} out of range [{DAILY_REVENUE_MIN:,.0f},{DAILY_REVENUE_MAX:,.0f}]"
            )
        if missing_ratio > MISSING_CUST_MAX:
            alerts.append(
                f"missing_customer_id_ratio={missing_ratio:.2%} > {MISSING_CUST_MAX:.2%}"
            )

        if alerts:
            status = "FAIL"
            msg = "⚠️ DQ Alert: " + " | ".join(alerts)
            print(msg)
            _send_slack(msg)
            _send_email(msg)
        else:
            print(
                f"✅ DQ OK: daily_total={daily_total:,.2f}, missing_ratio={missing_ratio:.2%}"
            )

        # 寫入監控表（若不存在則建立）
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dq_monitor_log (
                  check_ts TIMESTAMP DEFAULT NOW(),
                  check_date DATE,
                  daily_total NUMERIC,
                  missing_customer_ratio NUMERIC,
                  status TEXT,
                  alert_message TEXT
                );
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dq_monitor_log (check_date, daily_total, missing_customer_ratio, status, alert_message)
                VALUES (CURRENT_DATE, :t, :r, :s, :m)
                """
            ),
            {"t": daily_total, "r": missing_ratio, "s": status, "m": " | ".join(alerts)},
        )
        print("📝 已寫入 dq_monitor_log")


# ----------------------
# 可選：本檔單獨執行時的快速測試
# ----------------------
if __name__ == "__main__":
    try:
        df_raw = extract_data()
        df_tr = transform_data(df_raw)
        load_data(df_tr)
        quality_check()
        print("🎉 單檔測試完成")
    except Exception as e:
        print(f"❌ 發生錯誤：{e}")
        sys.exit(1)