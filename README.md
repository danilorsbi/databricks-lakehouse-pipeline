\# Databricks Lakehouse Pipeline



Production-grade Lakehouse pipeline built with Databricks, Delta Lake, and Medallion Architecture.



\## Architecture

\- Lakehouse Architecture(docs/images/lakehouse_pipeline.png)

## Medallion Architecture

This project implements the Medallion Architecture commonly used in Databricks Lakehouse environments.

### Bronze Layer
Responsible for ingesting raw data from source systems into Delta tables with minimal transformation.

### Silver Layer
Applies data cleaning, deduplication, and quality rules to create reliable datasets.

### Gold Layer
Provides business-ready aggregated datasets optimized for analytics and BI tools.



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

