# Customer Analysis ETL Pipeline Documentation

## 📄 Summary

This document provides an in-depth explanation of the design and implementation of the ETL (Extract, Transform, Load) pipeline used to perform customer data analysis. This pipeline is built using two main components, namely Apache Airflow as the orchestrator and Apache Spark as the data processing engine. The primary purpose of this pipeline is to extract raw customer activity and loyalty data from various sources, transform that data into more structured and meaningful information, and load the final results into a PostgreSQL-based data warehouse system. The output of this pipeline is an analytical summary table that can be utilized by business teams and data analysts to support data-driven strategic decision-making.

## 🛠️ Implementation Versions

During the development of this pipeline, two implementation approaches were created:

* **Version 1 (`assignment_dag.py`)**

  * In this version, the pipeline is built modularly by dividing each ETL (extract, transform, load, verify) stage into separate Python files. The purpose of this approach is to ensure separation of responsibilities, so that each file has a specific task.

* **Version 2 (`assignment_dag_2.py` and `assignment_etl.py`)**

  * In the second version, all ETL stages are created in a single Python file, namely `assignment_etl.py`. 

> This documentation focuses on **Version 2** because its structure and flow are easier to understand.

---

## 1. Airflow DAG Workflow

### 🔗 DAG Structure and Components

The pipeline is controlled by Apache Airflow, which uses DAG (Directed Acyclic Graph) as a blueprint to organize the execution order of interdependent tasks. Based on the `assignment_dag_2.py` file, the DAG has the following settings:

* **`dag_id`**: `customer_analytics_etl_dag_2`
* **Execution Trigger**: The DAG does not have a regular schedule (schedule=None) and only runs manually.
* **Main Tasks**:

1. `start_pipeline_2` – an `EmptyOperator` that serves as a marker that the ETL process has started.
  2. `etl_pipeline_2` – a `SparkSubmitOperator` that sends a command to run the ETL script on the Spark cluster.
  3. `end_pipeline_2` – an `EmptyOperator` that marks the end of the ETL process.
* **Dependency Flow**: The process is executed sequentially: `start_pipeline_2 >> etl_pipeline_2 >> end_pipeline_2`.

### ⚙️ Dynamic Parameterization

One of the key features of this pipeline is the flexibility to choose data transformation methods that can be customized directly from the Airflow interface. Users can specify a parameter named `load_method` to choose one of the following two transformation approaches:

* **`spark_jdbc`**: Data transformation is performed entirely using Spark. All transformation logic is written in PySpark and executed in parallel on the Spark cluster.
* **`postgres_pushdown`**: Transformation is performed on the PostgreSQL side, relying on complex SQL queries to manage and modify data.

This choice is passed to the Spark ETL script via the `application_args` argument, allowing the ETL logic to conditionally adjust the process based on the selected method.

![Parameter Display in Airflow](./images/Screenshot%202025-06-22%20103903.png)

### 📧 Notifications and Error Handling

To enhance system reliability and provide visibility to users, the pipeline is equipped with a notification system:

* **Automatic Notifications**:

* The `send_dag_notification` function is automatically triggered both when the pipeline runs successfully (`on_success_callback`) and when a failure occurs (`on_failure_callback`).

* **Failure Handling**:

  * If an error occurs during the ETL process, particularly when the `SparkSubmitOperator` task is executed, the task is automatically marked as failed. Airflow then triggers a callback to send a failure notification via the configured medium.

![Notification Display in Email](./images/Screenshot%202025-06-22%20095415.png)
---

## 2. ETL Process

The ETL stages are run by Apache Spark through the `assignment_etl.py` script, which contains all the process logic from data extraction, transformation, to loading and verification.

### 🚀 Initialization Stage

In the initial stage, the pipeline prepares the configuration and connections:

1. **`initialize_spark()`**: This function is responsible for creating and configuring the `SparkSession`, which serves as the primary interface for Spark to execute DataFrame operations.
2. **`get_jdbc_config()`**: This function retrieves credential information and the JDBC connection URL from the `.env` configuration file. This connection is used to interact with the PostgreSQL database.

### 🏗️ Extract Stage

* **Function**: `extract_data(spark)`
* **Process**: All cleaned source data in Parquet file format is stored in the `/data/cleaned` directory. This function automatically scans the directory and loads each subdirectory as a separate Spark DataFrame, such as `customer_loyalty_history` and `customer_flight_activity`. This ensures flexibility and scalability as the number of data sources increases.


### 🔄 Transformation Stage

Data transformation is a core part of the pipeline.

#### Method 1: Transformation in Spark (`spark_jdbc`)

* **Function**: `transform_data_in_spark(...)`
* **Process**:

* Perform joins between raw tables.
  * Calculate metrics such as lifetime total flights and accumulated points.
  * Classify customers based on `Churn` or `Retained` status.
* **Performance Optimization**:

  * Use `.persist()` to store the DataFrame temporarily in memory.
  * Apply `broadcast()` for efficient joins when one table is significantly smaller than the other.

#### Method 2: Transformation in PostgreSQL (`postgres_pushdown`)

* **Function**: `transform_data_in_postgres(...)`
* **Process**:

* First, data is loaded into a PostgreSQL staging table.
* Then, a single complex SQL query is executed to perform aggregation, joins, and calculations directly within the database.

### 💾 Load Stage

* **Function**: `load_data_to_postgres(...)`
* **Process**: The final result of the transformation process is loaded into the PostgreSQL data warehouse in `overwrite` mode. The final table name is dynamically generated based on the transformation method used, either `customer_summary_with_churn_spark` or `customer_summary_with_churn_sql`, but the schema remains the same.

![Email Notification Display](./images/Screenshot%202025-06-22%20095317.png)

### ✅ Verify Stage

* **Function**: `verify_data(...)`
* **Process**: After the data is loaded, the pipeline performs automatic verification by re-reading the table contents and displaying several rows as samples. Additionally, aggregation is performed based on customer status (Churn vs Retained) to ensure the data has been loaded correctly.

---

## 3. Batch Analysis Performed

This pipeline is designed not only to process data, but also to generate data conclusions.

### 🎯 Creation of a 360° Customer Profile

By consolidating data from various sources, the pipeline forms a unified summary table that provides a comprehensive view of each customer. This information includes:

* Demographic details such as age, gender, and region.
* Loyalty history including membership tier and total points earned.
* Behavioral metrics such as total number of flights and interaction frequency.


### 📊 Churn and Retention Analysis

* **Definition of Churn**: In this context, a customer is considered “Churned” if they have had no activity in the last year included in the dataset.
* **Analytical Output**:

* Each customer will be labeled as either `Churn` or `Retained`.
* This information allows companies to identify high-risk customers and devise interventions such as special offers or loyalty programs to prevent customer loss.