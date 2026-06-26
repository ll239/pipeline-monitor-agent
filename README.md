# Pipeline Monitor Agent

An AI-powered data pipeline monitoring system that detects quality issues, attempts auto-remediation using a STG/PROD pattern, and generates LLM-powered incident reports orchestrated by Apache Airflow.

## What it does

- Runs 6 automated quality checks on your pipelines including null rate, row count, schema drift, duplicates, freshness, and value range
- Fully config driven via monitor_config.yaml — point it at any database and table without touching code
- Validates data in a staging table before promoting to production — nothing hits PROD without passing checks
- Uses Claude (Anthropic) to reason about failures and generate human readable incident reports
- Escalates unresolvable issues by failing the Airflow DAG and triggering alerts
- Supports SQLite, Snowflake, Postgres, and BigQuery via connection factory

## Architecture

<img width="1122" height="1402" alt="ChatGPT Image Jun 26, 2026, 01_26_03 PM (1)" src="https://github.com/user-attachments/assets/8999c1a8-168a-4053-b9e1-a95e1f03cd58" />

The system runs as an Airflow DAG every 6 hours with 4 tasks in sequence:

1. detect_quality_issues — runs all checks against your pipeline tables
2. remediate_issues — attempts auto-fix using STG/PROD promotion pattern
3. generate_incident_report — Claude generates a human readable incident summary
4. check_escalation — fails the DAG if human intervention is needed

## Quick Start

Install dependencies:

    git clone https://github.com/ll239/pipeline-monitor-agent.git
    cd pipeline-monitor-agent
    pip install -r requirements.txt

Add your Anthropic API key to a .env file:

    ANTHROPIC_API_KEY=your-key-here

Simulate a pipeline with injected failures and run the monitor:

    python db/pipeline_sim.py
    python agent/reporter.py

Run the evaluation harness:

    python evals/scorer.py

## Configuration

Edit monitor_config.yaml to monitor your own tables. No code changes needed.

    connection:
      type: sqlite          # or snowflake, postgres, bigquery
      path: db/pipeline.db

    tables:
      - name: your_table
        staging_table: your_table_stg
        production_table: your_table_prod
        checks:
          null_rate:
            enabled: true
            column: revenue
            threshold: 0.1
            escalation_threshold: 0.3
            auto_remediate: true
          row_count:
            enabled: true
            min_expected: 1000
          schema_drift:
            enabled: true
            expected_columns:
              - id
              - revenue
              - created_at

    llm:
      model: claude-sonnet-4-6
      max_tokens: 1000

## Project Structure

    pipeline-agent/
      agent/
        detector.py        6 quality checks, reads thresholds from config
        remediator.py      STG/PROD auto-remediation, thresholds from config
        llm_agent.py       Claude failure analysis
        reporter.py        Structured incident report generation
      db/
        connection.py      Connection factory supporting SQLite, Snowflake, Postgres, BigQuery
        pipeline_sim.py    Pipeline simulator with failure injection
      dags/
        monitor_dag.py     Airflow DAG orchestrating all tasks
      evals/
        scorer.py          Evaluation harness with 3 failure scenarios
      monitor_config.yaml  Single config file for all settings

## Evaluation Results

The eval harness injects known failures and scores agent behavior:

    Null Revenue Injection    PASSED
    Row Count Drop            PASSED
    Schema Drift              PASSED

    Overall: 3/3 scenarios passed — Agent reliability: 100%

## Roadmap

    pip install pipeline-monitor-agent

Planning to package this as a pip installable library so any data team can
drop it into their existing Airflow DAG with a single import and one line of code.

## Tech Stack

- Python, SQLite, Apache Airflow
- Anthropic Claude claude-sonnet-4-6
- Astronomer Astro CLI
- PyYAML

## Author

Lina Devakumar Louis
github.com/ll239
