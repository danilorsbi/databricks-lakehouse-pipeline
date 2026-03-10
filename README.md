\# Databricks Lakehouse Pipeline



Production-grade Lakehouse pipeline built with Databricks, Delta Lake, and Medallion Architecture.



\## Architecture

\- Bronze layer for raw ingestion

\- Silver layer for cleansing, deduplication, quarantine, and MERGE

\- Gold layer for analytics-ready tables



\## Technologies

\- Databricks

\- Delta Lake

\- PySpark

\- Databricks Jobs

\- Medallion Architecture



\## Pipeline Flow

bronze\_ingest\_v2 → silver\_merge\_v2 → gold\_refresh\_v2



\## Author

Danilo Rodrigues

