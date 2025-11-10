**Retail ETL & Data Warehouse Pipeline**
本專案模擬零售銷售資料的自動化 ETL 流程與資料倉儲設計，
以 PostgreSQL 為資料倉儲核心，並透過 Prefect 實現每日排程與資料品質監控。

**專案架構**
本專案以 Online Retail II 資料集為例，模擬企業每日資料整併、清理與自動化上傳至資料倉儲的流程，並提供每日品質監控與警示。
資料來源：UCI	Online	Retail	II	(https://archive.ics.uci.edu/dataset/502/online+retail+ii)	（Year 2009–2010、Year 2010–2011）
資料處理流程：
1.擷取 (extract)：讀取多個年度的 Excel 資料
2.轉換 (transform)：欄位標準化、計算總額、識別退貨
3.載入 (load)：寫入 PostgreSQL
4.品質檢查 (data quality check)：每日檢測銷售總額與缺失比例
自動化排程：使用 Prefect 建立每日 01:00 自動執行排程

**系統需求**
Python版本：3.10 ~ 3.12 、3.14
資料庫：PostgreSQL 14+
主要套件：pandas, sqlalchemy, prefect, psycopg2, openpyxl

**Step 1：建立資料倉儲 Schema**
執行 dw_schema.sql 建立星型架構。
建立後的表結構如下：
例子：Schema Table名稱
      dw	dim_country
      dw	dim_product
      dw	dim_customer
      dw	dim_time
      dw	fact_sales

**Step 2：執行 ETL 流程**

手動執行 ETL：etl_pipeline.py

流程摘要：
1.擷取：自動讀取 Excel 多 Sheet 並合併
2.轉換：清理欄位、型別、退貨、異常值
3.載入：寫入 PostgreSQL → etl.sales_staging & etl.sales_cleaned
4.品質檢查：
  當日銷售總額是否落於 [1,000 ~ 5,000,000]
  缺失客戶 ID 比例是否 > 10%
  寫入 etl.dq_monitor_log 並可觸發 Slack / Email 警示

**Step 3：Prefect 自動化排程**

建立每日任務：
prefect deploy daily_sales_etl -n daily_sales_prod --cron "0 1 * * *" --timezone Asia/Taipei
啟動 Worker：
prefect worker start -p default-pool -q default

則會每日 01:00 自動執行 daily_sales_etl 流程，並同步進行品質檢查。

**Step 4：成果展示**
資料筆數統計
Table	            筆數
etl.sales_staging	1,067,371
etl.sales_cleaned	1,041,670

資料品質檢查結果
SELECT * FROM etl.dq_monitor_log ORDER BY check_ts DESC LIMIT 5;

check_date	daily_total	missing_ratio	status	alert_message
2025-11-06	0.00	       0.00	        FAIL	   daily_total=0.00 out of range [1,000,5,000,000]

**Step 5：專案擴充方向（Optional）**

*整合 Slack / Email 通知（即時 DQ 警示）
*連接 Power BI / Metabase 建立銷售監控儀表板
*改以 Cloud Scheduler 或 Docker 進行自動化部署
*增加歷史資料歸檔與每日差異比對
