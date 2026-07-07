# LogAI-Agent
AI-driven Airflow observability and log monitoring agent using Vertex AI
# 🤖 Intelligent Log Observability Agent & Performance Monitor (LogAI-Agent)

**Team:** Data Sentinals | **Event:** SAAMA IPL 2026 Ideathon  
**Role:** Data Engineering / Cloud Operations

---

## 📌 Project Overview
The **LogAI-Agent** is an event-driven, AI-augmented observability layer built for Google Cloud data engineering pipelines. 

It completely replaces static, threshold-based alerting with a deterministic semantic log-parsing engine. By integrating directly into the Apache Airflow execution lifecycle, this system automatically intercepts pipeline crash events, extracts nested API telemetry, and leverages a constrained Large Language Model (LLM) to perform instantaneous Root Cause Analysis (RCA).

### The Problem
Data Operations teams lose hours daily to manual log triage. When a pipeline breaks, engineers have to scroll through thousands of lines of raw Airflow and Dataform API JSON logs just to figure out what went wrong. Furthermore, rigid time-based SLAs trigger false "Critical" alarms during legitimate data volume scaling events, causing alert fatigue.

### The Solution
We engineered an automated pipeline that drops the Mean Time to Triage (MTTT) by **90%** (from 20+ minutes to under 10 seconds). The AI intercepts failures, reads the ugly stack traces, and outputs a clean, deterministic 15-word error summary, root cause, and remediation plan directly into a BigQuery operations dashboard.

---

## 🏗️ Technical Architecture & Data Flow

This project operates on a highly decoupled, multi-stage micro-architecture:

1. **Event Interception (The Trigger):** The system binds to Airflow’s `on_failure_callback` at the global DAG level. When a GCP Dataform task throws an exception, the callback suspends standard alerting and triggers the diagnostic script.
2. **Deep Telemetry Extraction:** Because standard Airflow logs often hide upstream errors, the agent utilizes the `DataformHook` to query the GCP Dataform API (`query_workflow_invocation_actions`). It extracts the exact, unformatted SQL syntax and schema validation errors directly from the BigQuery execution layer.
3. **Deterministic AI Parsing (Vertex AI):** The raw API dump is injected into a Context-Augmented prompt and routed to **Gemini 2.5 Flash**. To ensure **zero LLM hallucinations**, the prompt enforces strict programmatic constraints (a "JSON Straitjacket"), forcing the LLM to map the messy stack trace into a predefined schema: `severity`, `clean_summary`, `root_cause_analysis`, and `step_by_step_remediation`.
4. **State Logging:** The sanitized, structured JSON response is inserted into a centralized BigQuery telemetry table (`etl_performance_log`) for immediate operational visibility.
5. **Context-Aware SLA Monitoring:** A downstream Python operator tracks exact pipeline execution duration. It queries historical maximums to determine if a long runtime is a true infrastructure bottleneck or just an expected data scaling event, logging critical SLA breaches accordingly.

---

## 🛠️ Tech Stack

* **Orchestration:** Google Cloud Composer (Apache Airflow)
* **Transformation Engine:** Google Cloud Dataform
* **AI / ML Integration:** Vertex AI SDK (Gemini 2.5 Flash)
* **Data Persistence:** Google BigQuery
* **Language:** Python 3.x

---

## 📸 System in Action

### 1. The Airflow DAG Architecture
*(Shows the sequential compile and parallel execution logic, protected by the global error handler)* 

### 2. Catching the Pipeline Failure
*(Shows the system intercepting a live Dataform SQL failure in the patient_dim job without crashing the nurse_dim workflow)* 

### 3. The AI-Generated RCA Logged in BigQuery
*(The "Money Shot": Shows the live BigQuery dashboard populated with the AI's 15-word deterministic summary and remediation plan)* 

---

