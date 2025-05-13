# 📦 DAY 11 - ASSIGNMENT

## 🚀 Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline to retrieve news data from a data warehouse, clean and structure it for analysis, and load the processed data back into a dedicated table. The process ensures high-quality, well-formatted data suitable for downstream analytics or reporting.

> 🛢️ **Database used**: PostgreSQL

> 🧩 **Connected via**: DBeaver (a powerful database management tool for developers and analysts)

## 📁 Project Structure

The project is organized as follows:

```
.
├── extracted/                      # 🔍 Raw data extraction outputs
│   └── staging_data.parquet        # 📦 Staged data file
├── logs/                           # 📝 Pipeline logs
│   └── etl_pipeline.log            # 📖 Execution log file
├── transformed/                    # 🔄 Transformed data outputs
│   └── transformed_data.parquet    # 💾 Cleaned and processed data
├── config.yaml                     # 🗃 Pipeline and database configuration
├── etl.ipynb                       # 📊 Jupyter notebook for interactive ETL exploration
├── README.md                       # 📘 Project documentation (this file)
└── requirements.txt                # 📦 Python dependencies
```

## ✨ Features

* 📤 **Extract**: Read raw data from sources and write to Parquet (`extracted/staging_data.parquet`).
* 🧹 **Transform**: Clean, normalize, and enrich the data; output saved to `transformed/transformed_data.parquet`.
* 🧾 **Logging**: Record pipeline steps and errors in `logs/etl_pipeline.log`.
* 📓 **Interactive Notebook**: `etl.ipynb` for data exploration and ad hoc transformations.

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

4. **Configure settings**:

   * Edit `config.yaml` to adjust file paths, database credentials (if used), and other parameters.

## 🛠️ Usage

> 🎯 **Usage:** Run the entire pipeline interactively through Jupyter Notebook.

1. Launch the notebook:

   ```bash
   jupyter notebook etl.ipynb
   ```
2. Execute all cells (Run All) to perform extraction, transformation, and review the data end-to-end.

## 🔄 ETL Pipeline Workflow

1. **Extract** 📤: Retrieve required news fields (title, category, date, URL, summary) from the Data Warehouse on [Day 10 - Assignment](https://github.com/firdh0/Bootcamp_Dibimbing_Data-Engineering/tree/main/Day%2010_Data%20Warehouse%20Modeling/Assignment)
2. **Transform** 🧹: Cleanse, standardize, and enrich the data (remove duplicates, format dates, trim text, add processing timestamp).
3. **Load** 📦: Save the processed dataset back into a separate table in the Data Warehouse, ensuring no duplicates or data loss.

## 🔧 Configuration

The database connection and other configurations are stored in `config/config.yml`. Update this file with your database credentials and other settings.

Example `config.yml`:

```yaml
database:
  host: "localhost"
  database: "your_database"
  user: "your_username"
  password: "your_password"
```

## 🗒️ Logging

Logs are saved in `logs/etl_pipeline.log`. The log file contains detailed information about the pipeline's execution, including errors and warnings.

## 📄 Dependencies

The required Python libraries are listed in `requirements.txt`. Install them using:

```bash
pip install -r requirements.txt
```

## 📊 Example Output

### ✅ 1. Data Loaded to PostgreSQL (via DBeaver)

🖥️ *Visualized in DBeaver*

**Table: `dim_cleaned`**
![dim_category Table](./image/Screenshot%202025-05-13%20133956.png)
![dim_category Table](./image/Screenshot%202025-05-13%20134317.png)
