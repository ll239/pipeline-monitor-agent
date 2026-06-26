# Pipeline Monitor Agent

An AI-powered data pipeline monitoring system that detects quality issues, attempts auto-remediation, and generates LLM-powered incident reports — orchestrated by Apache Airflow.

## What it does

- Runs 6 automated quality checks on your pipelines — null rate, row count, schema drift, duplicates, freshness, and value range
- Validates data in a staging table before promoting to production — nothing hits PROD without passing checks
- Uses Claude (Anthropic) to reason about failures and write human-readable incident reports
- Escalates unresolvable issues by failing the Airflow DAG and triggering alerts
- Fully configurable via monitor_config.yaml — point it at any database and table

## Architecture

The system runs as an Airflow DAG every 6 hours with 4 tasks in sequence:

1. **detect_quality_issues** — runs all checks against your pipeline tables
2. **remediate_issues** — attempts auto-fix using STG/PROD promotion pattern
3. **generate_incident_report** — Claude generates a human-readable incident summary
4. **check_escalation** — fails the DAG if human intervention is needed

## Quick Start

**Install dependencies:**

    git clone https://github.com/ll239/pipeline-monitor-agent.git
    cd pipeline-monitor-agent
    pip install -r requirements.txt

**Add your Anthropic API key to a .env file:**

    ANTHROPIC_API_KEY=your-key-here

**Simulate a pipeline with injected failures and run the monitor:**

    python db/pipeline_sim.py
    python agent/reporter.py

**Run the evaluation harness:**

    python evals/scorer.py

## Configuration

Edit monitor_config.yaml to monitor your own tables:

    connection:
      type: sqlite
      path: db/pipeline.db

    tables:
      - name: your_table
        checks:
          null_rate:
            enabled: true
            column: your_column
            threshold: 0.1
          row_count:
            enabled: true
            min_expected: 1000

Supports SQLite, Snowflake, Postgres, and BigQuery connections.

## Project Structure

    pipeline-agent/
      agent/
        detector.py        6 quality checks, reads thresholds from config
        remediator.py      STG/PROD auto-remediation pattern
        llm_agent.py       Claude failure analysis
        reporter.py        Structured incident report generation
      db/
        pipeline_sim.py    Pipeline simulator with failure injection
      dags/
        monitor_dag.py     Airflow DAG orchestrating all tasks
      evals/
        scorer.py          Evaluation harness with 3 failure scenarios
      monitor_config.yaml  User configurable checks and thresholds

## Evaluation Results

The eval harness injects known failures and scores agent behavior across 3 scenarios:

    Null Revenue Injection    PASSED
    Row Count Drop            PASSED
    Schema Drift              PASSED

    Overall: 3/3 scenarios passed — Agent reliability: 100%

## Tech Stack

- Python, SQLite, Apache Airflow
- ChromaDB, Sentence Transformers
- Anthropic Claude (claude-sonnet-4-5)
- Astronomer Astro CLI

## Author

Lina Devakumar Louis
