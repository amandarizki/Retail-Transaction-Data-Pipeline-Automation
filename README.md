# Retail-Transaction-Data-Pipeline-Automation
This project implements a complete ETL (Extract, Transform, Load) automation pipeline for a retail transaction dataset using **PySpark**, **Apache Airflow**, and **MongoDB**.

## Overview

The pipeline includes:

- **Data extraction** from a CSV dataset.
- **Data transformation** (cleaning and enrichment).
- **Data validation** with Great Expectations.
- **Loading** into MongoDB.
- **Workflow orchestration** via Airflow.

## Project Structure

```

.
├── Information.md                     # Dataset description and link
├── Data_Raw.csv                       # Raw dataset
├── Extract.py                         # Extract script
├── Transform.py                       # Transform script
├── Load.py                            # Load script (safe with env variable)
├── DAG.py                             # Airflow DAG definition
├── GX.ipynb                           # Data exploration & validation notebook
├── DAG.jpg                            # Airflow DAG execution screenshot
└── README.md                          # Project documentation

````

## Objectives

- Automate ETL workflows.
- Validate and clean data.
- Load clean data into a NoSQL database.
- Enable scheduled, repeatable data ingestion.

---

## Dataset

**Retail Transaction Dataset**  
[Kaggle - Retail Transaction Dataset](https://www.kaggle.com/datasets/fahadrehman07/retail-transaction-dataset)

Features:
* >10 columns combining categorical and numeric fields.
* Columns with mixed-case naming (e.g., `StoreLocation`, `ProductCategory`, `DiscountApplied(%)`).
* Records of transactions including date, payment method, quantities, and discounts.


## Pipeline Components

### 1. Extract

**File:** `Extract.py`

- Reads raw CSV data with PySpark.
- Exposes `load_data()` returning a Spark DataFrame.


### 2. Transform

**File:** `Transform.py`

- Adds `TransactionID` column.
- Cleans newline characters in `StoreLocation`.
- Saves transformed data ready for loading.


### 3. Load

**File:** `Load.py`

- Converts Spark DataFrame to Pandas.
- Loads records into MongoDB using an environment variable for connection URI.

> **Important:** The MongoDB connection string is not hardcoded for security.  
You must set it via environment variable `MONGO_URI`.

Example usage:
```bash
export MONGO_URI="mongodb+srv://<username>:<password>@<cluster>.mongodb.net/"
````


### 4. Validation

**Notebook:** `GX.ipynb`

Performed validations:

* Unique composite key (`CustomerID`, `TransactionDate`, `ProductID`).
* Quantity within valid range.
* Payment method in allowed set.
* Data type checks.
* String length and regex validations.
* Date format checks.

---

### 5. Orchestration

**File:** `DAG.py`

* Airflow DAG with 3 tasks:

  * Extract
  * Transform
  * Load
* Schedule:

  * Every Saturday
  * Between 09:10–09:30 AM
  * Every 10 minutes

**You can customize:**

* **DAG ID** in `DAG.py`:

  ```python
  dag_id='milestone3_schedule'
  ```

  > Change e.g. `milestone3_schedule` to any unique name.

* **Description**:

  ```python
  description='ETL pipeline for crypto data every Saturday...'
  ```

  > You can update the description to match your dataset.

* **Script paths**:

  ```python
  bash_command='sudo -u airflow python /opt/airflow/scripts/extract.py'
  ```

  > If your scripts are stored elsewhere, adjust these paths.

* **File names**:

  * Raw dataset file:

    ```
    Data_Raw.csv
    ```
  * Notebook file:

    ```
    GX.ipynb
    ```
  * DAG graph screenshot:

    ```
    DAG.jpg
    ```

## How to Run

### Prerequisites

Install dependencies:

```bash
pip install pyspark apache-airflow pymongo great_expectations
```

---

### Airflow Setup

1. **Initialize and start Airflow:**

   ```bash
   airflow standalone
   ```

2. **Access the Airflow UI:**

   * URL: [http://localhost:8080](http://localhost:8080)

3. **Enable the DAG:**

   * Turn on the `milestone3_schedule` DAG.
     *If you renamed the DAG ID, look for your new DAG name.*

4. **Run or wait for schedule:**

   * You can trigger manually or wait for the scheduled execution.

---

### MongoDB Connection

Before running `load.py`, **set your MongoDB URI as an environment variable:**

**Linux/macOS:**

```bash
export MONGO_URI="mongodb+srv://<username>:<password>@<cluster>.mongodb.net/"
```

**Windows (PowerShell):**

```powershell
setx MONGO_URI "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/"
```

## Author

Amanda Rizki Koreana

---

*This project was developed to practice data engineering and analytics workflows.*

```
