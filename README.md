# Vehicle Telemetry Streaming Pipeline

## Overview
This project implements an end-to-end real-time data engineering pipeline for vehicle telemetry data.  
It ingests streaming events, processes them using a Bronze–Silver–Gold architecture with Delta Lake, and exposes analytics-ready data through Azure Synapse Serverless SQL.

The focus of this project is on **streaming ingestion, data modeling, and analytics exposure**, following production-oriented design principles.

---

## Architecture
Event Hub (Telemetry Simulator)  
→ Azure Databricks (Structured Streaming)  
→ ADLS Gen2 (Bronze / Silver / Gold – Delta Lake)  
→ Azure Synapse Serverless SQL (External Tables & Views)

---

## Tech Stack
- Python
- Azure Event Hubs
- Azure Databricks (PySpark, Structured Streaming)
- Delta Lake
- Azure Data Lake Storage Gen2
- Azure Synapse Analytics (Serverless SQL)

---

## Data Processing Layers
**Bronze**
- Raw ingestion of telemetry events from Event Hubs
- Schema-on-read, minimal transformations

**Silver**
- Data cleansing and type casting
- Timestamp normalization and basic validations

**Gold**
- Aggregated, analytics-ready datasets using event-time windowing
- 5-minute window aggregations per vehicle:
  - Average speed
  - Maximum engine temperature
  - Event count

---

## Synapse Analytics
- Gold Delta datasets are exposed via **external tables** in Synapse Serverless SQL
- **Views** are created on top of external tables for downstream analytics consumption
- Views act as the semantic layer and are intended for BI/reporting tools

---

## Repository Structure
- simulator/     - Event Hub telemetry producer
- databricks/    - Streaming ingestion and transformation notebooks
- synapse/       - Synapse external tables and view definitions (SQL)
- README.md

---

## How to Run
1. Start the telemetry simulator to publish events to Azure Event Hubs.
2. Execute Databricks notebooks in order:
   - ADLS configuration
   - Bronze ingestion
   - Silver transformations
   - Gold aggregations
3. Run the Synapse SQL script to create external tables and views.

---

## Impact
- Scalable structured streaming ingestion handling continuous telemetry events
- Near real-time, analytics-ready datasets exposed via Synapse Serverless SQL
- Fault-tolerant design using checkpointing and schema enforcement with Delta Lake

---

## Design Decisions
- Event-time processing with watermarking is used to handle late-arriving data.
- Delta Lake is used to ensure ACID guarantees and support upserts.
- Synapse Serverless SQL is used to expose analytics-ready data without moving data.
- Views are used as a semantic layer for downstream analytics consumption.

---

## Limitations and Future Enhancements
- Data quality validations can be extended with additional rules and alerts.
- CI/CD for Databricks and Synapse artifacts is not implemented in this project.
- Gold-layer modeling can be enhanced using dimensional modeling patterns.
- Monitoring and cost optimization can be added for production scale.


