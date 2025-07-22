
# 🚀 GoFood ETL Medallion Pipeline & Dashboard

This documentation explains the end-to-end data pipeline implementation for collecting data from the GoFood platform, processing it through a Medallion architecture, storing it in Google BigQuery, and serving it via an interactive web dashboard based on ReactJS.

## 🏗️ Architecture & Technology

The data workflow for this project follows the Medallion Architecture, ensuring data is processed incrementally from raw to analysis-ready.

### ➡️ Data Flow:
GoFood Web (Source) → Web Scraper → GCS (Bronze) → Spark (Transformation) → GCS (Silver) → BigQuery (Gold) → Backend API → ReactJS Frontend (Dashboard)

### 🛠️ Key Technologies Used:
* 🐳 **Containerization**: Docker & Docker Compose
* ⚙️ **Workflow Orchestration**: Apache Airflow
* 🐍 **Programming Languages**: Python (for scraping and Spark transformation), JavaScript/Node.js (for backend API), JavaScript/ReactJS (for frontend)
* 🕸️ **Web Scraping**: Selenium WebDriver & BeautifulSoup
* ☁️ **Cloud Storage**: Google Cloud Storage (GCS)
* 📦 **Data Warehouse**: Google BigQuery
* ⚡ **Distributed Data Processing**: Apache Spark (via PySpark)
* 📜 **Incremental Metadata Storage**: Google Cloud Datastore
* ↩️ **Backend API**: Node.js with Express
* 🌐 **Frontend Web**: ReactJS with Chart.js (via react-chartjs-2)

## 📁 Project Structure & Files Used

The project structure is designed modularly to separate pipeline components:

```

your-project-root/
├── dags/
│   └── gofood\_etl\_dag.py             \# Airflow DAG definition for ETL orchestration
├── docker/
│   ├── Dockerfile                    \# Dockerfile for scraper image
│   ├── Dockerfile.airflow            \# Dockerfile for custom Airflow image
│   └── docker-compose.yml            \# Docker Compose configuration for Airflow & Spark cluster
├── frontend/
│   └── gofood-dashboard/             \# ReactJS frontend web application
│       ├── public/
│       ├── src/                      \# Main React application components
│       │   ├── App.js
│       │   ├── index.js
│       │   ├── App.css
│       │   └── components/
│       │       ├── Login.js          \# Login form component
│       │       ├── Dashboard.js      \# Main dashboard component with visualizations
│       │       ├── Login.css
│       │       └── Dashboard.css
│       └── package.json
├── backend/                          \# Node.js API backend application
│   ├── server.js                     \# Express.js server for API
│   ├── .env                          \# Environment variables for backend (local)
│   └── package.json
├── scraper/
│   ├── main.py                       \# Python code for web scraping
│   ├── gofood\_scraper.py             \# Main scraping logic
│   ├── ulasan             \# Class for scraping restaurant details, menu, promo, reviews
│   ├── gofood\_navigator.py           \# Class for GoFood web navigation
│   ├── browser\_manager.py            \# Class for managing Selenium WebDriver
│   ├── gcloud\_datastore\_manager.py   \# Class for Datastore interaction
│   ├── gcs\_uploader.py               \# Class for uploading data to GCS
│   └── requirements.txt              \# Python dependencies for scraper
├── spark-scripts/
│   ├── main.py                       \# PySpark code for data transformation
│   ├── spark\_transformer.py          \# Main Spark Job logic
│   ├── spark\_session\_manager.py      \# Class for data transformation to dimension/fact models
│   └── (other PySpark files)         \# Class for managing SparkSession
├── secret/                           \# Folder for sensitive credentials (e.g., gcp\_credentials.json)
│   └── gcp\_credentials.json          \# GCP service account key
├── .env                              \# Environment variables for Docker Compose (project root)
└── README.md

````

## ⚙️ Component Details & Workflow

### 🕸️ 1. Extraction Phase (Web Scraping)
* **Code Location**: `scraper/`
* **Tools**: Python, Selenium WebDriver (Chrome), BeautifulSoup, Google Cloud Datastore, Google Cloud Storage.
* **Process**:
    * `gofood_etl_dag.py` (Airflow) runs the `extract_to_bronze` task using `DockerOperator`.
    * `main.py` inside the `gofood-scraper:latest` container orchestrates the scraping process.
    * `GoFoodNavigator` automates browser navigation to GoFood, selects the current location, and clicks the 'Terdekat' (Nearby) category.
    * `GoFoodScraper` is at the core of data retrieval. It visits each found restaurant page and extracts details (name, rating, distance, address, opening hours), menu items, promotions, and reviews.
    * **Incremental Review Scraping (via Google Cloud Datastore)**: For reviews, `GoFoodScraper` interacts with `gcloud_datastore_manager.py`. It retrieves the timestamp of the last successfully scraped review per restaurant from Datastore. Only reviews newer than this timestamp are processed. If no new reviews exist, an empty CSV file (header only) is uploaded to GCS Bronze. Datastore serves as stateful storage to track the last scraping timestamp, enabling incremental data ingestion.
    * `GCSUploader` is responsible for uploading all scraped data (restaurants, menus, promos, reviews) to the GCS Bronze Layer in CSV format, within a folder specific to the current DAG run (`bronze/{run_datetime_str_full}/`).

### ✨ 2. Transformation Phase (Spark Job)
* **Code Location**: `spark-scripts/`
* **Tools**: Apache Spark (PySpark), Google Cloud Storage, Google BigQuery.
* **Process**:
    * `gofood_etl_dag.py` runs the `transform_bronze_to_silver` task using `SparkSubmitOperator`.
    * `main.py` inside the Spark container executes the Spark Transformer.
    * The Spark Transformer reads raw CSV data from GCS Bronze for the current DAG run.
    * **Data Cleaning**: Data is deduplicated (removing exact duplicate rows across all columns) and null values are filled with appropriate placeholders.
    * **Data Type Conversion & Price Formatting**: Price columns (`Harga`) are converted from strings (e.g., "40", "71.5", "150") to numeric FLOAT values in full Rupiah (e.g., 40000.0, 71500.0, 150000.0) by multiplying by 1000.0. Date columns (`Tanggal Beli`) are parsed from "DD Month YYYY" format using `to_timestamp` with `locale="id-ID"` and cast to `DateType`.
    * **Data Modeling**: Data is transformed and modeled into dimension tables (`dim_restaurant`, `dim_menu`, `dim_promotion`, `dim_date`) and a fact table (`fact_transaction`) following a star schema.
    * **Incremental ID Generation**: For each dimension and fact table, Spark connects to BigQuery to get the maximum existing ID. New IDs are then generated incrementally (`MAX_ID_lama + dense_rank()`) to ensure uniqueness and ID continuity.
    * The transformation results are written to the GCS Silver Layer in Parquet format (`silver/{run_datetime_str_full}/`).

### 📥 3. Loading Phase (BigQuery)
* **Code Location**: `dags/gofood_etl_dag.py`
* **Tools**: Apache Airflow, Google Cloud Storage, Google BigQuery.
* **Process**:
    * After Spark transformation completes, Airflow runs a series of tasks to load data into BigQuery.
    * **Main Dimension Table Initialization**: The `initialize_merge_tables_tasks` uses `GCSToBigQueryOperator` with `WRITE_TRUNCATE` to ensure main dimension tables (`dim_restaurant`, `dim_menu`, `dim_promotion`) are created (if they don't exist) or their schema is refreshed from the latest Parquet data in Silver.
    * **Fact & Date Dimension Table Loading**: For `fact_transaction` and `dim_date`, `GCSToBigQueryOperator` is used with `WRITE_APPEND`, appending new data from Silver to the existing BigQuery tables.
    * **Dimension Table Loading & Merging**: For `dim_restaurant`, `dim_menu`, `dim_promotion`, data from Silver is first loaded into temporary staging tables in BigQuery (`_staging`) using `WRITE_TRUNCATE`.
        * Then, `BigQueryInsertJobOperator` executes a `MERGE INTO` statement in BigQuery.
        * `MERGE` will update existing records in the main table if there are changes (based on business keys like restaurant/menu/promo name).
        * `MERGE` will insert new records if not found in the main table.
        * Specifically for `dim_promotion`, `MERGE` will also deactivate promotions that were previously active but are no longer found in the latest data batch.

### 📊 4. Serving Phase (Backend API & Frontend Web)
* **Code Location**: `backend/`, `frontend/`
* **Tools**: Node.js (Express), ReactJS, Chart.js.
* **Process**:
    * **Backend API** (`server.js`):
        * Exposes HTTP endpoints (e.g., `/api/reviews-by-date`, `/api/greatest-savings-promos`, `/api/cheapest-restaurants`, `/api/rating-correlation-data`, `/api/best-value-recommendations`).
        * Receives requests from the frontend.
        * Executes SQL queries to BigQuery to retrieve relevant data.
        * Returns data in JSON format.
        * **"Best Value" Recommendation**: Receives a budget from the user, filters BigQuery data based on that budget (using full values), and returns a list of suitable menu/promo recommendations.
    * **Frontend Web** (`Dashboard.js`):
        * Provides a web interface for login and displaying the dashboard.
        * Retrieves user location from the browser.
        * Makes API calls to the backend to retrieve visualization data.
        * **Price Formatting on Frontend**: All price values received from the backend (which are already in full values, e.g., 50000.0) are formatted into "Rp 50.000" strings using a `formatRupiah` function for user-friendly display.
        * Renders data visualizations using Chart.js (Bar Chart, Scatter Plot) and interactive tables.

## 🚀 How to Use This Project

Here are the detailed steps to set up and run the entire GoFood data pipeline and web dashboard locally using Docker Compose.

### 🛠️ Step 1: Environment Preparation (Prerequisites)
Ensure you have the following software installed on your local machine:
* **Git**: For cloning the project repository.
* **Docker Desktop**: For running all services in containers (Airflow, Spark, Scraper). Ensure Docker Desktop is running and has sufficient resources (RAM, CPU).
* **Node.js & npm (or Yarn)**: Required for running the Node.js backend application and ReactJS frontend locally. Download from [nodejs.org](https://nodejs.org/).
* **Google Cloud SDK (gcloud CLI)**: For authenticating to Google Cloud and interacting with GCS/BigQuery/Datastore from the terminal. Follow the installation guide [here](https://cloud.google.com/sdk/docs/install). After installation, run `gcloud init` and `gcloud auth application-default login` to authenticate your CLI to your GCP account.

### 🔑 Step 2: Google Cloud Platform (GCP) Credential Configuration
This project requires access to GCP services (GCS, BigQuery, Datastore).

1.  **Create a Service Account**:
    * Open Google Cloud Console.
    * Navigate to `IAM & Admin > Service Accounts`.
    * Click `+ CREATE SERVICE ACCOUNT`.
    * Provide a name (e.g., `gofood-etl-sa`).
    * Grant the following roles:
        * `Storage Admin` (for GCS)
        * `BigQuery Data Editor` (for writing to BigQuery)
        * `BigQuery Job User` (for running BigQuery jobs)
        * `Cloud Datastore User` (for reading/writing to Datastore)
    * Click `DONE`.

2.  **Create a JSON Key File**:
    * Once the Service Account is created, click on its name in the list.
    * Select the `Keys` tab.
    * Click `ADD KEY > Create new key`.
    * Choose `JSON` as the key type, then click `CREATE`.
    * A JSON file will be downloaded to your computer (e.g., `your-project-id-xxxxxxxxxxxx.json`).

3.  **Place the JSON Key**:
    * Create a `secret/` folder in your project root if it doesn't already exist.
    * Move the downloaded JSON file to the `secret/` folder and rename it to `gcp_credentials.json`.
    * **Example Path**: `your-project-root/secret/gcp_credentials.json`

4.  **Configure the `.env` File**:
    * Open the `.env` file in your project root (`your-project-root/.env`).
    * Populate it with your Project ID and the absolute path to your credential file. Ensure the absolute path is correct for your operating system (Windows/Linux/macOS).

    ```bash
    #.env (in project root)
    GCP_KEYFILE_ON_HOST=C:\\Users\\LEGION\\Music\\GitHub\\Bootcamp_Dibimbing_Data-Engineering\\FINAL_PROJECT2\\secret\\gcp_credentials.json
    # Or for Linux/macOS:
    # /path/to/your/project-root/secret/gcp_credentials.json
    GCP_KEYFILE_IN_SCRAPER=/tmp/gcp_credentials.json
    GCP_KEYFILE_IN_CONTAINER=/opt/airflow/secret/gcp_credentials.json
    GCS_BUCKET=gofood-data-lake-bucket
    BIGQUERY_PROJECT_ID=gofood-465817
    BIGQUERY_DATASET_ID=gofood_analytics
    ```
    * **Important**: Replace `gofood-465817` with your actual GCP Project ID.

### 🐳 Step 3: Build Docker Images
You need to build Docker images for the scraper and Airflow/Spark.

1.  **Build Scraper Image**:
    * Open your terminal in your project root (`your-project-root/`).
    * Run the following command:
        ```bash
        docker build -t gofood-scraper:latest -f docker/Dockerfile scraper/
        ```
    * (This command assumes the Dockerfile for the scraper is in `docker/Dockerfile` and the build context is the `scraper/` folder).

2.  **Build Airflow & Spark Images**:
    * Still in your project root.
    * Run the command:
        ```bash
        docker build -t gofood-airflow:latest -f docker/Dockerfile.airflow .
        ```

### 🚀 Step 4: Run Infrastructure (Docker Compose)
Once the Docker images are built, you can run the entire Airflow, Spark Master, and Spark Worker infrastructure using Docker Compose.

* In your terminal (still in the project root), run:
    ```bash
    docker-compose -f docker/docker-compose.yml up -d
    ```
* This will start all services in the background (`-d`).
* Wait a few minutes until all services (especially Airflow Webserver and Scheduler) are fully active. You can monitor logs with `docker-compose -f docker/docker-compose.yml logs -f`.

### 🔌 Step 5: Setup Airflow Connections
Airflow needs to know how to connect to Google Cloud.

1.  **Access Airflow UI**:
    * Open your browser and navigate to [http://localhost:8080](http://localhost:8080).
    * Log in with username `airflow` and password `airflow`.

2.  **Configure GCP Connection**:
    * In the Airflow UI, navigate to `Admin > Connections`.
    * Look for a connection with `Conn Id: google_cloud_default`. If it doesn't exist, click `+` to create a new one.
    * **Edit or Create Connection**:
        * `Conn Id`: `google_cloud_default`
        * `Conn Type`: `Google Cloud`
        * `Project Id`: Enter your GCP Project ID (e.g., `gofood-465817`).
        * **Authentication Method (Important!)**:
            * `Keyfile Path`: Masukkan jalur di dalam container Airflow ke file kredensial JSON Anda: `/opt/airflow/secret/gcp_credentials.json`.
            * `Scopes`: `https://www.googleapis.com/auth/cloud-platform`
    * Click `Test` to ensure the connection is successful, then `Save`.

### 💻 Step 6: Run Local Backend API
This is the server that will serve data from BigQuery to the frontend.

1.  **Navigate to Backend Folder**:
    * Open a new terminal.
    * Navigate to the `backend/` directory:
        ```bash
        cd C:\Users\LEGION\Music\GitHub\Bootcamp_Dibimbing_Data-Engineering\FINAL_PROJECT2\backend
        ```

2.  **Install Dependencies**:
    ```bash
    npm install
    ```

3.  **Configure `.env` File for Backend**:
    * Create a `.env` file inside the `backend/` folder (if it doesn't already exist).
    * Populate it with the absolute path to your credentials and Project ID:
        ```bash
        #backend/.env
        GOOGLE_APPLICATION_CREDENTIALS=C:\\Users\\LEGION\\Music\\GitHub\\Bootcamp_Dibimbing_Data-Engineering\\FINAL_PROJECT2\\secret\\gcp_credentials.json
        # Or for Linux/macOS:
        # /path/to/your/project-root/secret/gcp_credentials.json
        BIGQUERY_PROJECT_ID=gofood-465817
        BIGQUERY_DATASET_ID=gofood_analytics
        PORT=3001
        ```

4.  **Run Backend Server**:
    ```bash
    node server.js
    ```
    * The server will run at [http://localhost:3001](http://localhost:3001). Leave this terminal open.

### 🌐 Step 7: Run Local ReactJS Frontend
This is the dashboard application you will see in your browser.

1.  **Navigate to Frontend Folder**:
    * Open a new terminal.
    * Navigate to the `frontend/gofood-dashboard/` directory:
        ```bash
        cd C:\Users\LEGION\Music\GitHub\Bootcamp_Dibimbing_Data-Engineering\FINAL_PROJECT2\frontend\gofood-dashboard
        ```

2.  **Install Dependencies**:
    ```bash
    npm install
    ```

3.  **Verify Proxy Configuration**:
    * Ensure the `frontend/gofood-dashboard/package.json` file has the line `"proxy": "http://localhost:3001"` in it.

4.  **Run Frontend Application**:
    ```bash
    npm start
    ```
    * This will open your React application in the browser (usually [http://localhost:3000](http://localhost:3000)). Leave this terminal open.

### ▶️ Step 8: Start ETL Pipeline (Trigger DAG)
Once all infrastructure is running and the frontend/backend are ready, you can trigger the data pipeline.

1.  Return to the Airflow UI ([http://localhost:8080](http://localhost:8080)).
2.  Find the DAG named `gofood_etl_medallion_pipeline`.
3.  Ensure the DAG is enabled (toggle button is blue).
4.  Click the Play/Trigger DAG button (triangle icon) next to the DAG name.
5.  Select "Trigger DAG" (without additional configuration if not needed).
6.  Monitor the task status in the Airflow UI. All tasks should turn green (SUCCESS). You can view logs for each task for debugging if there are issues.

### 📈 Step 9: Access the Dashboard
After the DAG has successfully run at least once and populated your BigQuery with data, you can access the web dashboard.

1.  Open your browser and navigate to [http://localhost:3000](http://localhost:3000).
2.  Log in with username `admin` and password `admin123`.
3.  You should see the dashboard with your location data, and data visualizations from BigQuery loaded via the backend API. You can also try the "Best Value" Recommendation feature by entering a budget.

By following these steps, you will have the entire GoFood data pipeline running locally! 🎉

## 🎯 Platform Results and Capabilities

This built GoFood data pipeline platform is capable of effectively processing and serving data from web sources to BigQuery data warehouse, making it ready for in-depth analysis and visualization.

### ✅ Platform Capabilities:
* **📝 Automated & Incremental Data Ingestion**: The system can perform scheduled and incremental web scraping of GoFood for reviews, ensuring data is always up-to-date and resource-efficient.
* **🛡️ Robust Data Transformation**: With Apache Spark, raw data is cleaned, structured, and modeled into an analysis-ready star schema (dimensions and facts). Handling of duplicates, missing values, and data format inconsistencies (including prices and dates) has been implemented, resulting in high-quality data.
* **🔄 Incremental & Consistent Data Loading**: Data is loaded into BigQuery efficiently using `APPEND` strategy for facts and `MERGE` for dimensions, ensuring data consistency and integrity over time. The feature to deactivate irrelevant promotions is an example of advanced incremental capabilities.
* **📈 Interactive Data Presentation**: The ReactJS-based web dashboard can display various insights through data visualizations and interactive tables, including:
    * 📉 Review trends.
    * 💰 List of promotions with the greatest savings.
    * 🍽️ Cheapest restaurants per category.
    * 🔗 Data for analyzing the correlation of ratings with prices and promotions.
    * 💡 "Best Value" recommendations based on user budget, filtering and displaying menus/promos that fit the entered budget.

### ⚠️ Platform Limitations and Challenges:
Although this platform is robust, there are some inherent limitations and challenges to consider:
* **🕸️ High Dependency on Web Structure**: The web scraping method is highly vulnerable to changes in the HTML structure of GoFood web pages. Any small change in layout or HTML element names can break the scraper, requiring continuous manual maintenance.
* **🐌 Scalability and Scraping Performance**: Although efficient for the current data volume, Selenium-based web scraping can become a performance bottleneck and consume significant resources if the number of restaurants or reviews to be scraped increases drastically.
* **🗑️ Varying Source Data Quality**: Despite cleaning processes, highly unstructured or invalid data from web sources may still require more complex transformation logic or even manual intervention to ensure 100% accuracy.
* **🤔 Promotion Association Accuracy**: Associating promotions with restaurants (`dp.name LIKE CONCAT('%', dr.name, '%')`) is a fuzzy matching method. This has the potential to cause inaccuracies if the promo name does not explicitly contain the restaurant name or if there are restaurants with very similar names. Ideally, promotions would have clear restaurant IDs from their source.
* **⚠️ Spark Performance (Window Functions)**: The `WARN WindowExec: No Partition Defined for Window operation!` warning in Spark logs indicates potential performance issues if the data volume in Spark becomes very large and Window operations are not properly partitioned. This can lead to bottlenecks and high memory usage on Spark workers.
* **🔗 Container Network Connectivity**: Network connectivity issues from Docker containers to the internet or Google Cloud services (as seen from `Could not reach host` or `Connection refused` errors) can disrupt the scraping and Datastore update processes.
* **🚫 Non-UTF-8 Characters**: The presence of non-standard or surrogate characters in scraped data can cause `UnicodeEncodeError` during logging or data writing, although it does not always stop the main pipeline.