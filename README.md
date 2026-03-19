# Real Estate ETL - 台灣不動產房地產實價登錄自動化平台

這是一個基於 **Apache Airflow (Astro CLI)** 與 **Apache Spark** 的自動化 ETL 平台，專門處理台灣內政部實價登錄資料。系統採用大數據分層架構，將原始資料從政府官網爬取後，存儲於 MinIO 物件存儲中，再由 Spark 進行資料清洗與轉換，最後載入 PostgreSQL 資料庫，並透過Power BI 進行分析。


---

## 🚀 功能特色 | Features

### 資料工程管道 | Data Engineering Pipeline
- ✅ **自動化爬蟲 (Automated Crawler)**: 定期從內政部官網下載最新季度的 ZIP 壓縮檔。
- ✅ **物件存儲預處理 (MinIO Staging)**: 自動解壓並將原始 CSV 上傳至 S3 相容儲存層（MinIO）。
- ✅ **分散式處理 (Spark Transformation)**: 利用 Pyspark 進行大規模資料清洗、結構強化與欄位轉換。
- ✅ **結構化存儲 (PostgreSQL Storage)**: 將轉換後的資料持久化至關聯式資料庫。
- ✅ **Power BI 資料分析 (Power BI Visualization)**: 資料存儲於 PostgreSQL 後，直接透過 Power BI 進行報表製作與洞察分析。

### 資料處理細節 | Data Processing Details
- ✅ **自動單位轉換**: 支援平方公尺與「坪」的自動換算。
- ✅ **日期標準化**: 自動將民國年轉換為西元紀年。
- ✅ **資料清理**: 自動過濾英文表頭及髒資料。
- ✅ **歷史溯源**: 記錄資料來源年度與季度。

---

## 📂 專案結構 | Project Structure

```text
airflow_realEstate/
├── dags/
│   └── realestate.py          # Airflow DAG: 定義完整 ETL 流程
├── spark/
│   ├── master/                # Spark Master 配置
│   ├── worker/                # Spark Worker 配置
│   └── notebooks/
│       └── realestate_transform/
│           └── realestate_transform.py  # Spark 核心轉換邏輯
├── streamlit-app/
│   └── app.py                 # (已停用) 原 Streamlit 佔位程式
├── PowerBI/                   # (建議) 存放 Power BI (.pbix) 檔案之目錄
├── docker/                    # 基礎設施 Dockerfile 與配置
├── include/                   # 自定義輔助函式或靜態檔案
├── airflow_settings.yaml      # Airflow 連線與變數配置
├── docker-compose.override.yml # 擴充服務 (Postgres, MinIO, Spark)
├── requirements.txt           # Python 依賴套件
└── packages.txt               # OS 層級相依套件
```

---

## 🛠️ 安裝步驟 | Installation Steps

### 1. 複製專案與準備環境 | Clone and Setup
```bash
# 下載專案
git clone <your-repo-url>
cd airflow_realEstate

# 確保已安裝 Astro CLI
# https://www.astronomer.io/docs/astro/cli/get-started
```

### 2. 配置環境變數 | Configuration
請建立或編輯 `.env` 檔案，請參考.env.example


### 3. 啟動系統 | Start the Platform
使用 Astro CLI 啟動所有容器服務（Airflow, Spark, Postgres, MinIO）：
```bash
astro dev start
```
啟動後可透過以下介面管理系統：
- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (Account: `admin` / `admin`)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001)
- **Spark Master**: [http://localhost:8081](http://localhost:8081)

### 4. 執行任務 | Run the Task
1. 登入 Airflow UI。
2. 啟動名為 `realEstate` 的 DAG。
3. 監控任務執行，完成後資料將匯入 PostgreSQL 的 `real_estate_table` 表中。

---

## 🏗️ 系統架構 | System Architecture


    A[MOI Website] -->|Requests| B(Airflow Crawler)
    B -->|Upload Raw| C[MinIO Storage]
    B -->|Trigger Job| D(Spark Cluster)
    D -->|Read & Clean| C
    D -->|JDBC Load| E[(PostgreSQL)]
    E -->|Direct Query| F[Power BI Dashboard]

  
```

---

## 👨‍💻 技術棧 | Tech Stack
- **Orchestration**: Apache Airflow
- **Processing**: Apache Spark (PySpark)
- **Object Storage**: MinIO
- **Database**: PostgreSQL
- **Visualization**: Power BI
- **Language**: Python 3.10+
- **Infrastructure**: Docker & Docker Compose
