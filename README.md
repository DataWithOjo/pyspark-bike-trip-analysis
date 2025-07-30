# Spark Bike Trip Analysis

This project uses **PySpark** to ingest zipped CSV files containing bike trip data, perform aggregations, and generate CSV reports. It demonstrates core big data concepts such as distributed processing, modular ETL design, and batch analytics.

## 🚀 Project Summary

- 📥 Read zipped CSV data using PySpark.
- 📊 Generate daily and monthly analytical reports.
- 🧪 Modular function design, with optional unit tests.
- 📂 Output results as CSV files into a `reports/` directory.

## 🛠️ Tech Stack

- Python 3
- Apache Spark (PySpark)
- Docker & Docker Compose
- `unittest` (optional)

## 📦 Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/spark-bike-trip-analysis.git
cd spark-bike-trip-analysis
docker build --tag=exercise-6 .
docker-compose up
