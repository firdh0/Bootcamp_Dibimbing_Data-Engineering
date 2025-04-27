# dbt Project Documentation

## 1. Setting Up the Virtual Environment
Create and activate a Python virtual environment:
```powershell
python -m venv environment_name
.\environment_name\Scripts\activate
```

**Notes**:  
- Make sure to use the correct environment name when activating.  
- Initial activation errors may occur if the environment folder name is incorrect.

## 2. Installing dbt
Install the dbt core package and the PostgreSQL adapter:
```powershell
pip install dbt-core dbt-postgres
```

**Main installed dependencies**:
- dbt-core==1.9.4
- dbt-postgres==1.9.0
- psycopg2-binary==2.9.10
- Jinja2==3.1.6

## 3. Initializing the dbt Project
Create a new project and configure the database connection:
```powershell
dbt init Dibimbing
```

**PostgreSQL profile configuration**:
```yaml
host: ep-plain-dust-a16eucaa-pooler.ap-southeast-1.aws.neon.tech
port: 5432
user: neondb_owner
password: neondb
schema: classicmodel
threads: 1
```

## 4. Running the Model
Execute the first dbt model:
```powershell
dbt run
```

**Successful output**:
```
PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

## 5. Managing Packages
Install additional packages:
```powershell
dbt deps
```

**Installed packages**:
- calogica/dbt_expectations@0.8.5 (deprecated)
- dbt-labs/dbt_utils@1.0.0
- calogica/dbt_date@0.7.2

**Warnings**:
- The dbt_expectations package is deprecated; migration to metaplane/dbt_expectations is recommended.
- Newer versions are available for all dependencies.

## 6. Running a Specific Model
Run the `fact_sales` model:
```powershell
dbt run --models fact_sales
```

**Successful output**:
```
SELECT 538 in 0.81s
PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

## 7. Generating Documentation
Create and serve project documentation:
```powershell
dbt docs generate
dbt docs serve
```

**Access the documentation**:
```
http://localhost:8080
```
![Alt text](./Screenshot%202025-04-27%20122226.png)
