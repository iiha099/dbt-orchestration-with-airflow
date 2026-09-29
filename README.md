# Airflow dbt BigQuery Pipeline

## Overview

This project demonstrates how to orchestrate dbt workflows using Apache Airflow.

Airflow is used to schedule and manage dbt transformations, while dbt builds and tests data models stored in Google BigQuery.

## Technologies

- Apache Airflow
- dbt Core
- Google BigQuery
- Docker
- Python

## Pipeline
Airflow
|
↓
dbt run
|
↓
dbt test
|
↓
BigQuery
## Features

- Run dbt jobs through Airflow DAGs
- Automate dbt model execution and testing
- Generate dynamic DAG tasks using dbt manifest.json
- Integrate Airflow with Google BigQuery

## Project Structure
dags/
dbt_lewagon/
tests/
Dockerfile
docker-compose.yml
## Run

```bash
docker compose up
Run DAGs from the Airflow UI.
Testing
make test
Skills Practiced
Data pipeline orchestration
Airflow and dbt integration
Dynamic DAG generation
Cloud data warehouse workflows
