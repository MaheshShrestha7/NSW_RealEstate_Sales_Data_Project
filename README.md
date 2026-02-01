# New South Wales Property Sales Data – Python ETL Pipeline

## 📌 Project Overview
This project implements a **Python ETL (Extract, Transform, Load) pipeline** for ingesting, cleaning, and curating **New South Wales Government Property Sales data** (Australia) openly available and published by the NSW Valuer General.

The goal is to demonstrate **real‑world data engineering practices**, including:
- Clear separation of Extract, Transform, and Load responsibilities
- Modular, testable Python code
- Logging and observability
- Parameter‑driven execution
- Replayable and auditable data pipelines

This project is designed as **Project 1: Python ETL Foundation** in a Modern Data Engineering learning path.

---

## 🗂️ Data Source
- **Publisher:** [NSW Valuer General](https://valuation.property.nsw.gov.au/embed/propertySalesInformation)
- **Dataset:** NSW Property Sales Information
- **Access Pattern:** Static yearly ZIP files
- **URL Pattern:**
  ```
  https://www.valuergeneral.nsw.gov.au/__psi/yearly/{YEAR}.zip
  ```

### File Characteristics
- Nested ZIP files (Year → Weekly ZIPs → `.dat` files)
- Delimited flat ASCII format (`;` separator)
- Encoding: `cp1252`
- Multiple record types (`A`, `B`, `C`, `D`, `Z`)
- **This pipeline processes Record Type `B` only** (property address & sales data)

---

## 🏗️ High‑Level System Architecture

```
                +-----------------------------+
                |  NSW Valuer General Website |
                |  (Static Yearly ZIP Files)  |
                +--------------+--------------+
                               |
                               | HTTP Download
                               v
+----------------------+   +-----------------------+
|  Extract Layer (E)   |-->|  Raw Data (Bronze)    |
|  - Download ZIPs     |   |  data/raw/*.csv       |
|  - Parse nested ZIPs |   +-----------------------+
|  - Filter B records  |
+----------+-----------+
           |
           v
+----------------------+ 
| Transform Layer (T)  |
| - Type casting       |   
| - Standardisation    |
| - Business rules     |
| - Deduplication      |
+----------+-----------+
           |
           v
+----------------------+    +-----------------------+
|   Load Layer (L)     | -->| Curated Data (Silver) |
| - CSV output         |    | data/curated/*.csv    |
|                      |    +-----------------------+
+----------------------+

        +----------------------------------+
        | Centralised Logging (logs/etl.log)|
        +----------------------------------+
```

---

## 📁 Project Structure

```
NSW_RealEstate_Sales_Data_Project/
├── etl/
│   ├── extract/
│   │   └── nsw_sales_extract.py
│   ├── transform/
│   │   └── nsw_sales_transform.py
│   ├── load/
│   │   └── nsw_sales_load_to_csv.py
│   ├── logger.py
│   └── pipeline.py
├── data/
│   ├── raw/
│   └── curated/
├── docs/
├── logs/
│   └── etl.log
├── tests/
└── README.md
```

---

## 🔄 ETL Design

### 1️⃣ Extract (E)
**File:** `etl/extract/nsw_sales_extract.py`

**Responsibilities:**
- Download yearly ZIP file
- Extract nested weekly ZIPs
- Parse `.dat` files
- Filter **Record Type `B` only**
- Return a raw pandas DataFrame

**Design Principle:**
> Extract performs *faithful ingestion only*. No business logic.

---

### 2️⃣ Transform (T)
**File:** `etl/transform/nsw_sales_transform.py`

**Responsibilities:**
- Type casting (prices, dates, area)
- Text standardisation (street, locality)
- Business rules enforcement
- Deduplication
- Data quality filtering

**Examples of Rules:**
- Purchase price must be > 0
- Contract date must be present
- Duplicate sales removed using business keys

---

### 3️⃣ Load (L)
**File:** `etl/load/nsw_sales_load_to_csv.py`

**Responsibilities:**
- Persist curated data to CSV
- Create directories automatically
- Log record counts

> Loader is environment‑agnostic and can be extended to PostgreSQL, Redshift, BigQuery, etc.

---

## 📊 Data Layers

| Layer | Purpose | Location |
|-----|--------|---------|
| Raw (Bronze) | Immutable source data | `data/raw/` |
| Curated (Silver) | Clean, analytics‑ready | `data/curated/` |

### Curated (Silver) meta-data

Data columns (total 24 columns):
| #  | Column Name          | Data Type | Non-Null Count | Description                                      |
| -- | -------------------- | --------- | -------------- | ------------------------------------------------ |
| 0  | `Record_Type`        | string    | 199,909        | Record identifier (always `B` for sales records) |
| 1  | `District_Code`      | int       | 199,909        | Valuation district code                          |
| 2  | `Property_Id`        | float     | 199,801        | Unique identifier for the property               |
| 3  | `Sale_Counter`       | int       | 199,909        | Sale sequence number for the property            |
| 4  | `Download_Date`      | string    | 199,909        | Date the record was published/downloaded         |
| 5  | `Property_Name`      | string    | 5,933          | Name of the property (if applicable)             |
| 6  | `Unit_No`            | string    | 66,052         | Unit or apartment number                         |
| 7  | `House_No`           | string    | 196,780        | House or street number                           |
| 8  | `Street_Name`        | string    | 199,580        | Street name                                      |
| 9  | `Locality`           | string    | 199,905        | Suburb or locality                               |
| 10 | `Postcode`           | int       | 199,909        | Postal code                                      |
| 11 | `Area`               | float     | 159,858        | Land area value                                  |
| 12 | `Area_Type`          | string    | 159,891        | Unit of area measurement (e.g. square metres)    |
| 13 | `Contract_Date`      | string    | 199,909        | Contract date (YYYYMMDD format)                  |
| 14 | `Settlement_Date`    | string    | 199,908        | Settlement date (YYYYMMDD format)                |
| 15 | `Purchase_Price`     | int       | 199,909        | Final sale price                                 |
| 16 | `Zoning`             | string    | 127,436        | Zoning classification                            |
| 17 | `Nature_of_Property` | string    | 199,909        | Property type indicator                          |
| 18 | `Primary_Purpose`    | string    | 199,883        | Intended primary use of the property             |
| 19 | `Strata_Lot_No`      | float     | 67,911         | Strata lot number (if applicable)                |
| 20 | `Component_Code`     | string    | 127,436        | Component classification code                    |
| 21 | `Sale_Code`          | string    | 445            | Sale condition or classification code            |
| 22 | `Interest_Share`     | float     | 79,365         | Percentage ownership interest                    |
| 23 | `Dealing_No`         | string    | 199,909        | Legal dealing / transaction reference            |

dtypes: float64(4), int64(4), str(16)

---

## 📜 Logging & Observability

- Centralised logging via `etl/logger.py`
- Logs written to:
  - Console
  - `logs/etl.log`

**Each ETL stage logs:**
- Start & completion
- Record counts
- Errors with stack traces



---

## ▶️ How to Run the Pipeline

### Prerequisites
- Python 3.10+
- Virtual environment recommended

### Install Dependencies
```bash
pip install pandas requests
```

### Run from Project Root
```bash
python -m etl.pipeline --year 2025
```

### Output
- Raw CSV: `data/raw/nsw_sales_2025.csv`
- Curated CSV: `data/curated/nsw_sales_2025.csv`
- Logs: `logs/etl.log`

---

## 🧠 Key Engineering Concepts Demonstrated

- Modular ETL architecture
- Separation of concerns
- Idempotent pipeline design
- Parameter‑driven execution
- Replayable historical runs
- Production‑grade logging

---

## 🚀 Future Enhancements

Planned or easy extensions:
- Incremental weekly files load
- Database / warehouse loading
- Row‑count reconciliation 
- Data quality metrics
- Airflow orchestration

---

## 👤 Author
Built as part of a **Modern Data Engineering** learning journey by [Mahesh Shrestha](https://datawithmahesh.com/) 

This project intentionally mirrors **real industry Python ETL patterns**.

---

## 📄 License
Data is provided by the [NSW Government](https://valuation.property.nsw.gov.au/). 
This project is for **educational purposes only**.
