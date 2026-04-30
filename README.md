# Cryptocurrency ETL Quality Engineering Pipeline

## Executive Summary

This repository presents a production-style ETL framework for cryptocurrency market data, designed with explicit quality controls at each stage of the data lifecycle. The solution demonstrates how to combine ingestion, transformation, validation, and reporting into an auditable workflow suitable for analytics operations.

From a leadership perspective, the focus is not only on data movement, but on data trust: quality gates, business-rule enforcement, and automated testing are embedded into pipeline execution so that poor-quality records are detected early and prevented from entering downstream reporting.

## Strategic Objectives

- Deliver a repeatable ETL workflow that can be operated and tested consistently.
- Enforce measurable quality standards before and after transformation.
- Provide transparent audit artifacts for governance and troubleshooting.
- Support multiple output formats (`parquet`, `csv`, `json`) for broad compatibility.
- Maintain a test-first implementation with strong automated validation.

## Architecture

```text
CoinGecko API
   |
   v
Ingestion Layer -> Quality Gate 1 (Raw Data) -> Raw Storage
   |
   v
Transformation Layer -> Quality Gate 2 (Processed Data)
   |
   +--> Processed Outputs (Parquet/CSV/JSON)
   +--> Quality Reports and Execution Metadata
```

## Data Quality Model

The `DataQualityValidator` applies a four-domain framework:

1. Completeness
   - Verifies critical fields and threshold compliance.
2. Accuracy
   - Enforces business constraints (for example, positive prices, reasonable volume behavior).
3. Consistency
   - Validates formatting and standardization rules.
4. Uniqueness
   - Identifies duplicate keys and integrity violations.

Quality gates are configurable by stage (raw vs. processed), enabling stricter control as data approaches publication.

## Core Components

### `src/data_ingestion/`
- `api_client.py`: resilient API client with retry/error-handling behavior.
- `data_fetcher.py`: ingestion orchestration and persistence of raw snapshots.

### `src/etl_pipeline/`
- `pandas_pipeline.py`: no-Java ETL path for broad local compatibility.
- `pipeline_runner.py` and related PySpark modules: distributed ETL execution path.
- `integrated_pipeline.py`: orchestrates ETL with integrated quality gates.

### `src/data_quality/`
- `quality_validator.py`: quality scoring, validation results, and report generation.

### `src/utils/`
- environment/session utilities (including Spark session configuration for local execution).

## Engineering Capabilities Demonstrated

- Multi-stage ETL orchestration with formal pass/fail quality controls.
- Rule-based data quality scoring with actionable outputs.
- Automated persistence of quality reports for auditability.
- Structured error handling and execution metadata for observability.
- Support for both Pandas-based and PySpark-based execution modes.

## Testing and Quality Assurance

The project includes unit, integration, and end-to-end execution tests across ingestion, quality validation, and ETL orchestration.

Representative command:

```bash
pytest -v --cov=src --cov-report=html
```

Recent local validation in this workspace completed successfully with all collected tests passing.

## Quick Start

### Prerequisites

- Python 3.11+ recommended
- Dependencies from `requirements.txt`
- Optional: Java runtime for the full PySpark path

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Run Key Workflows

```bash
python test_integrated_pipeline.py
python test_quality_framework.py
pytest -q
```

## Operational Outputs

Pipeline runs generate:

- Raw data snapshots
- Processed datasets (`parquet`, `csv`, `json`)
- Raw and processed quality reports (JSON)
- Execution metadata and quality scores

Primary output locations:

- `data/raw/`
- `data/processed/`
- `data/quality_reports/`

## Business Impact

This implementation aligns with practical ETL quality engineering goals:

- Risk reduction: quality gates reduce propagation of invalid records.
- Reliability: repeatable execution with automated verification.
- Governance: auditable reports and traceable run artifacts.
- Maintainability: modular design with focused test coverage.

## Repository Structure

```text
src/
  data_ingestion/
  data_quality/
  etl_pipeline/
  utils/
tests/
data/
requirements.txt
README.md
```

## Repository

`https://github.com/JUnelus/ETL-Automation-Testing-with-PySpark---Cryptocurrency-Data-Pipeline`
