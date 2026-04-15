# 📊 Netflix Content Analysis using PySpark, Hive & HDFS

## 📌 Project Overview

This project performs large-scale analysis of Netflix content to uncover trends, patterns, and insights across production, genres, regions, and viewer preferences. Using distributed data processing frameworks, the analysis demonstrates how big data tools can be applied to real-world media datasets.

---

## 🎯 Objectives

* Analyze the growth of movies and TV shows over time
* Identify regional content distribution and trends
* Explore genre popularity across different countries
* Understand viewer preferences through content duration and ratings

---

## 🛠️ Tech Stack

* **PySpark** – Data processing and transformation
* **Hadoop (HDFS)** – Distributed data storage
* **Hive** – Querying and analytical processing
* **Tableau** – Data visualization and dashboards

---

## 📂 Project Structure

```
netflix-data-analysis/
│
├── data/                  # Dataset (or sample data)
├── scripts/               # PySpark scripts
│   ├── data_cleaning.py
│   ├── data_aggregation.py
│
├── queries/               # Hive SQL queries
│   └── analysis.sql
│
├── visualizations/        # Tableau dashboards / images
├── notebooks/             # (Optional) exploratory notebooks
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

## ⚙️ Data Processing Pipeline

### 1. Data Ingestion

* Loaded raw Netflix dataset into HDFS
* Structured data for distributed processing

### 2. Data Cleaning & Transformation

* Handled missing values and null records
* Removed duplicates and irrelevant columns
* Standardized string formats
* Extracted numerical features from duration fields

### 3. Data Aggregation

Performed large-scale aggregations using PySpark:

* Yearly production trends (Movies vs TV Shows)
* Regional distribution by country and genre
* Genre popularity over time
* Duration-based viewer preference analysis

### 4. Analytical Queries (Hive)

* Growth rate analysis of content over time
* Genre distribution across regions
* Content popularity based on ratings and country
* Comparative analysis of movies vs TV shows

---

## 📊 Key Insights

* Significant increase in TV show production in recent years
* Content distribution varies heavily across regions
* Certain genres consistently dominate global popularity
* Viewer preferences indicate trends in content duration

---

## 📈 Data Visualization

Interactive dashboards were created using Tableau to present:

* Production trends over time
* Genre popularity and regional distribution
* Viewer preference patterns

*(Add screenshots or dashboard links here)*

---

## ▶️ How to Run

### Prerequisites

* Hadoop & HDFS setup
* Apache Spark
* Hive

### Steps

```bash
# Run data cleaning
spark-submit scripts/data_cleaning.py

# Run data aggregation
spark-submit scripts/data_aggregation.py
```

---

## 🚀 Future Improvements

* Integrate real-time streaming data (e.g., Kafka)
* Deploy pipeline using cloud platforms (AWS / Azure)
* Build predictive models for content success

---

## 📄 License

This project is for academic and educational purposes.
