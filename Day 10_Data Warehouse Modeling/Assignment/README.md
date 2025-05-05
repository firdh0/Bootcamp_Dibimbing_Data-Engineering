# 📦 DAY 10 - ASSIGNMENT

## 🚀 Project Overview

This project is an implementation of an **ETL (Extract, Transform, Load)** pipeline designed to process raw data, transform it into a structured format, and load it into a database. The pipeline is modular, allowing for easy customization and scalability.

> 🛢️ **Database used**: PostgreSQL

> 🧩 **Connected via**: DBeaver (a powerful database management tool for developers and analysts)

## ⭐ Database Star Schema Design
![dim_category Table](./images/Schema%20Database-Star%20Schema.drawio.png)

## 📁 Project Structure

The project is organized as follows:

```
.
├── config/
│   └── db_config.yml          # 🛠️ Configuration file for database connection
├── data/
│   ├── raw/                   # 📥 Directory for raw data
│   │   └── bbc_news.csv       # 📰 Example raw data file
│   ├── processed/
│       ├── staging_data.parquet      # 🗂️ Staging data file
│       └── transformed_data.parquet  # 🔄 Transformed data file
├── scripts/
│   ├── etl/
│   │   ├── extract.py         # 📤 Data extraction logic
│   │   ├── transform.py       # 🧹 Data transformation logic
│   │   ├── load.py            # 📦 Data loading logic
│   ├── utils/
│   │   ├── logger.py          # 🧾 Logging utility
│   │   ├── db_connection.py   # 🔌 Database connection utility
│   │   └── data_quality.py    # ✅ Data quality checks
│   ├── logs/
│   │   └── etl_pipeline.log   # 🗒️ Log file for ETL pipeline
├── main.py                    # ▶️ Main script to run the ETL pipeline
├── requirements.txt           # 📄 Python dependencies
└── README.md                  # 📘 Project documentation
```

## ✨ Features

* 📤 **Data Extraction**: Reads raw data from CSV files and converts it into a staging format (Parquet).
* 🧹 **Data Transformation**: Cleans and processes the data, ensuring quality and consistency.
* 📦 **Data Loading**: Loads the transformed data into **dimension and fact tables in a PostgreSQL database**.
* 🧾 **Logging**: Provides detailed logs for monitoring and debugging.

## ✅ Prerequisites

* 🐍 Python 3.10 or higher
* 🐘 PostgreSQL database
* 🖥️ DBeaver for managing and exploring the database
* 📦 Required Python libraries (see `requirements.txt`)

## ⚙️ Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Set up a virtual environment:

   ```bash
   python -m venv venv
   venv\Scripts\activate  # On Windows
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Configure the database connection in `config/db_config.yml`.

## 🛠️ Usage

1. Place the raw data file (`bbc_news.csv`) in the `data/raw/` directory.
2. Run the ETL pipeline:

   ```bash
   python main.py
   ```

## 🔄 ETL Pipeline Workflow

1. **Extract** 📤: Reads raw data from `data/raw/bbc_news.csv` and saves it as `data/processed/staging_data.parquet`.
2. **Transform** 🧹: Processes the staging data, performs quality checks, and saves the transformed data as `data/processed/transformed_data.parquet`.
3. **Load** 📦: Loads the transformed data into **dimension and fact tables** in a PostgreSQL database that is managed via **DBeaver**.

## 🔧 Configuration

The database connection and other configurations are stored in `config/db_config.yml`. Update this file with your database credentials and other settings.

Example `db_config.yml`:

```yaml
database:
  host: "localhost"
  database: "your_database"
  user: "your_username"
  password: "your_password"
```

## 🗒️ Logging

Logs are saved in `scripts/logs/etl_pipeline.log`. The log file contains detailed information about the pipeline's execution, including errors and warnings.

## 📄 Dependencies

The required Python libraries are listed in `requirements.txt`. Install them using:

```bash
pip install -r requirements.txt
```

---

## 📊 Example Output

### ✅ 1. Data Loaded to PostgreSQL (via DBeaver)

🖥️ *Visualized in DBeaver*

**Table: `dim_category`**
![dim_category Table](./images/Screenshot%202025-05-05%20093308.png)

**Table: `fact_news`**
![dim_category Table](./images/Screenshot%202025-05-05%20093520.png)

---
