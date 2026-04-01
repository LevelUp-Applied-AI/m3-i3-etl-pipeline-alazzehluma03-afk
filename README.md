[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

<!-- What does this pipeline do? -->
This ETL pipeline is designed to process digital market data from Amman. It extracts raw information from a PostgreSQL database, applies business logic to calculate customer metrics, and ensures data quality before exporting the results for analysis.
## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

<!-- What does customer_analytics.csv contain? -->
The pipeline generates a specialized CSV report (customer_analytics.csv) and a database table containing:

Customer Identity: Names and IDs of active customers.

Sales Metrics: Total revenue and unique order counts.

Behavioral Insights: Average order value and the customer's favorite product category.
## Quality Checks

<!-- What validations are performed and why? -->
To maintain high data standards, the following automated checks are performed:

Null Value Prevention: Critical fields like customer_id must be populated.

Financial Integrity: All recorded revenue must be greater than zero.

Data Deduplication: Ensures that each customer has a unique entry in the final output.
---

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
