# Python Project for Data Engineering

This repository contains the final project developed for the **IBM Data Engineering professional certification** on Coursera.  
The project focuses on the design and implementation of an end-to-end **ETL (Extract, Transform, Load) pipeline** using Python, following core data engineering principles.

---

## 📌 Project Description

The objective of this project is to automate the ingestion of data from a web source, transform it into a structured and consistent format, and load it into a relational database for further analysis.  
The pipeline also includes logging and monitoring mechanisms to ensure traceability and robustness during execution.

---

## ETL Pipeline Overview

The project follows a classic ETL architecture:

### 1. Extraction

- Data is collected via **web scraping** using Python.
- The extraction process retrieves raw data from an online source and prepares it for processing.

### 2. Transformation

- Raw data is cleaned, normalized, and structured.
- Transformation logic ensures consistency and readiness for relational storage.

### 3. Loading

- Transformed data is loaded into a **SQLite database** (`.db` file).
- Database schema and tables are created programmatically when required.

### 4. Querying

- SQL queries are executed on the database to validate data integrity and retrieve insights.

---

## 📝 Logging

A custom logging system is implemented to monitor the ETL process:

- Logs track the execution of each ETL stage.
- Errors and execution timestamps are recorded.
- Logging follows standard Python best practices for maintainability and debugging.

---

## 🛠️ Technologies Used

- **Python**
- **Web Scraping (Requests, BeautifulSoup)**
- **SQLite**
- **SQL**

---

## 🎯 Key Features

- End-to-end ETL pipeline development
- Web data extraction
- Data cleaning and transformation
- Relational database loading
- SQL querying and validation
- Application-level logging and monitoring