# Differential Privacy Data Pipeline on Azure

## 🚀 Overview

This project implements an **end-to-end data pipeline on Microsoft Azure** to process user event data while **enforcing Differential Privacy (DP)** guarantees.

The pipeline follows a **Bronze / Silver / Gold** architecture and is fully containerized.  
It is designed to run as an **Azure Container Apps Job**, automatically triggered when new data arrives in the Bronze layer.

Key objectives:
- Build a scalable analytics pipeline
- Apply Differential Privacy using **PipelineDP**
- Ensure cloud-native execution (Docker, Azure Blob Storage, Spark)
- Respect data minimization and privacy-by-design principles

---

## 🏗️ High-Level Architecture

┌──────────────┐
│ Event Source │
└──────┬───────┘
       │ JSON events
       ▼
┌────────────────────┐
│ Azure Blob Storage │
│  Bronze Container  │
└──────┬─────────────┘
       │ Trigger (Timer Function)
       ▼
┌──────────────────────────────┐
│ Azure Container Apps Job     │
│  - Docker                    │
│  - Spark                     │
│  - PipelineDP                │
└──────┬─────────────┬────────┘
       │             │
       ▼             ▼
┌──────────────┐  ┌──────────────┐
│ Silver Layer │  │ Gold Layer   │
│ Clean Data   │  │ DP Analytics │
└──────────────┘  └──────────────┘


---

## 📂 Repository Structure

├── dp_app/
│   ├── bronze_reader.py      # Read & flatten raw JSON events
│   ├── silver_layer.py       # Spark transformations (clean session data)
│   ├── dp_analysis.py        # Baseline + Differential Privacy analytics
│   ├── gold_writer.py        # Persist results & plots to Gold
│   ├── spark_utils.py        # Spark session configuration
│   ├── env.py                # Environment & Azure configuration
│   ├── logging_utils.py      # Structured logging
│   └── main.py               # Pipeline orchestration
│
├── dp-trigger-controller/    # Azure Function (Timer-triggered)
│
├── Dockerfile                # Container image definition
├── requirements.txt          # Python dependencies
├── main.py                   # Container entrypoint
└── README.md




---

## 🥉 Bronze Layer — Raw Data

- Stores **raw JSON event files**
- No transformation
- Immutable source of truth
- Triggers the pipeline when new data arrives

Example fields:
- device
- geoNetwork
- summary metrics

---

## 🥈 Silver Layer — Clean Data

Purpose:
- Normalize and clean data
- One row per session
- Typed, analytics-ready format

Technologies:
- Apache Spark
- Parquet storage

Example columns:
- continent
- country
- device
- browser
- num_pageviews
- total_time_on_page

---

## 🥇 Gold Layer — Privacy-Preserving Analytics

Only **aggregated data** is exposed.

Implemented analyses:
- Number of sessions per continent (DP COUNT)
- Average session duration per device (DP MEAN)

Outputs:
- JSON result files
- Comparison plots (raw vs DP)

No user-level or session-level data is stored in Gold.

---

## 🔐 Differential Privacy Design

Differential Privacy is enforced using **PipelineDP**.

Key constraints:
- Bounded user contribution
- Noise calibrated using ε (epsilon)
- Explicit value bounds for numerical metrics

Example configuration:

AggregateParams(
    metrics=[Metrics.MEAN],
    min_value=0,
    max_value=600_000,
    max_partitions_contributed=1,
    max_contributions_per_partition=1
)



## ⚙️ Deployment Workflow
Build the Docker image
docker build -t dp-pipeline .

Push to Azure Container Registry
docker tag dp-pipeline dpsimacr.azurecr.io/dp-pipeline:v1
docker push dpsimacr.azurecr.io/dp-pipeline:v1

Run the Container Apps Job
az containerapp job start \
  -name dp-pipeline-job \
  -resource-group rg-dp-sim


## 🔁 Automation & Triggering

An Azure Function (Timer Trigger):

Periodically scans the Bronze container

Detects new event files

Triggers the Container Apps Job

Uses a checkpoint to avoid reprocessing data

## 🧠 What This Project Demonstrates

Cloud-native data engineering

Apache Spark in containers

Azure Blob Storage & Container Apps

Practical Differential Privacy

Secure, reproducible analytics pipelines

## 📌 Possible Extensions

- CI/CD with GitHub Actions
- Advanced privacy budget accounting
- Schema evolution management
- Visualization dashboards (Power BI, Looker)