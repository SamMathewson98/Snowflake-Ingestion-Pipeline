# Healthcare Claims Analytics Pipeline (Snowflake)

This project demonstrates an end-to-end analytics pipeline for ingesting semi-structured healthcare claims data into Snowflake, dynamically extracting schema, and producing query-ready, population-health–focused analytics.

The pipeline is designed to mirror real-world healthcare data warehouse patterns: schema drift tolerance, safe typing, and analytics-first modeling.

---

## Architecture Overview

Parquet / Semi-Structured Source
↓
RAW Landing Table (schema-agnostic, string-based)
↓
Snowpark Python Dynamic Schema Extraction
↓
Clean SQL Table (one column per JSON key)
↓
Curated Typed View (dates, numerics, identifiers)
↓
Population Health Analytics Queries


---

## Pipeline Components

### 1. Raw Ingestion Layer
- Source data arrives as **Parquet / JSON-like records**
- Data is landed into Snowflake without enforcing schema
- All values are preserved as strings to prevent ingestion failures

**Why:**  
Healthcare data schemas evolve frequently. Preserving raw records ensures resilience and auditability.

---

### 2. Dynamic Schema Extraction (Snowpark Python)
- Snowpark Python script:
  - Parses JSON-encoded rows
  - Dynamically discovers keys across records
  - Normalizes keys into valid Snowflake column names
  - Produces a SQL table with one column per field
- No hard-coded column list required

**Key features:**
- Handles escaped JSON
- Handles schema drift
- Fully Snowflake-native (no external compute)

---

### 3. Curated Analytics Layer
- A curated SQL view applies:
  - Safe type casting using `TRY_TO_DATE`, `TRY_TO_DECIMAL`, `TRY_TO_NUMBER`
  - Explicit separation of identifiers vs numeric measures
  - Rounding for financial fields

**Healthcare-aware modeling choices:**
- NPIs, CPTs, ICD codes remain strings
- ZIP3 retained as string (leading zero safe)
- Financial fields rounded to two decimals
- Dates standardized for time-series analytics

---

## Analytics & Population Health Queries

The repository includes advanced Snowflake SQL queries supporting population health and cost analytics, including:

### Example Metrics
- Monthly paid and allowed trends
- PMPM (Per Member Per Month) cost
- High-cost claimant concentration (Pareto analysis)
- Outlier spend detection using statistical thresholds
- ED utilization proxies via CPT codes
- Preventive vs non-preventive spend mix
- Geographic (ZIP3) cost variation
- Provider billing variation
- Chronic condition / comorbidity burden proxies

### Query Patterns Used
- CTE-driven modeling (`WITH base AS (...)`)
- Window functions (`AVG() OVER`, `STDDEV() OVER`)
- Approximate distinct counting for scale
- Robust filtering with `QUALIFY`
- Explicit grain control (claim-, patient-, provider-level)

---

## Technologies Used

- **Snowflake**
  - SQL analytics
  - Window functions
  - CTE-based modeling
- **Snowpark (Python)**
  - Dynamic schema extraction
  - JSON parsing
  - Automated column generation
- **Healthcare Analytics Concepts**
  - PMPM
  - Utilization
  - Outlier detection
  - Population segmentation

---

## Design Principles

- **Schema resilience** over brittle ingestion
- **Separation of concerns**
  - Raw → Clean → Curated
- **Safe casting**
  - No query failures due to bad data
- **Auditability**
  - Raw data preserved
- **Analytics-first**
  - Queries designed for population health use cases

---

