from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# add agent folder to path so we can import our modules
sys.path.append("/Users/linalouis/PycharmProjects/pipeline-agent/agent")
sys.path.append("/Users/linalouis/PycharmProjects/pipeline-agent/db")

from detector import run_all_checks
from remediator import remediate
from reporter import generate_report

# -------------------- DAG DEFAULT ARGS --------------------
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# -------------------- TASK FUNCTIONS --------------------
def run_detector(**context):
    """Task 1 — Run all quality checks"""
    print("Starting pipeline quality checks...")
    check_results = run_all_checks()

    # push results to XCom so next task can access them
    context["ti"].xcom_push(key="check_results", value=check_results)

    failed = [c for c in check_results if not c["passed"]]
    print(f"Checks complete. {len(failed)} failed out of {len(check_results)}")
    return check_results


def run_remediator(**context):
    """Task 2 — Attempt remediation for failed checks"""
    # pull check results from previous task via XCom
    check_results = context["ti"].xcom_pull(
        task_ids="detect_quality_issues",
        key="check_results"
    )

    print("Starting remediation...")
    remediation_results = remediate(check_results)

    # push remediation results to XCom
    context["ti"].xcom_push(key="remediation_results", value=remediation_results)

    escalations = [r for r in remediation_results if r["requires_escalation"]]
    print(f"Remediation complete. {len(escalations)} issues need escalation.")
    return remediation_results


def run_reporter(**context):
    """Task 3 — Generate incident report"""
    # pull results from previous tasks via XCom
    check_results = context["ti"].xcom_pull(
        task_ids="detect_quality_issues",
        key="check_results"
    )
    remediation_results = context["ti"].xcom_pull(
        task_ids="remediate_issues",
        key="remediation_results"
    )

    print("Generating incident report...")
    report = generate_report(check_results, remediation_results)

    print(f"Report generated: {report['report_id']}")
    print(f"Status: {report['status']}")
    return report["report_id"]


def check_escalation(**context):
    """Task 4 — Fail DAG if escalation required"""
    remediation_results = context["ti"].xcom_pull(
        task_ids="remediate_issues",
        key="remediation_results"
    )

    escalations = [r for r in remediation_results if r["requires_escalation"]]

    if escalations:
        print(f"ESCALATION REQUIRED — {len(escalations)} issues need attention")
        print("Failing DAG to trigger Airflow alerts...")
        # raising an exception fails the task and triggers email alert
        raise Exception(
            f"Pipeline monitor requires escalation for: "
            f"{[e['check'] for e in escalations]}"
        )
    else:
        print("All issues auto remediated. Pipeline healthy.")


# -------------------- DAG DEFINITION --------------------
with DAG(
        dag_id="pipeline_monitor_agent",
        default_args=default_args,
        description="AI powered pipeline monitoring agent",
        schedule="0 */6 * * *",  # runs every 6 hours
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["monitoring", "data_quality", "ai_agent"]
) as dag:
    detect = PythonOperator(
        task_id="detect_quality_issues",
        python_callable=run_detector,
    )

    remediate_task = PythonOperator(
        task_id="remediate_issues",
        python_callable=run_remediator,
    )

    report = PythonOperator(
        task_id="generate_incident_report",
        python_callable=run_reporter,
    )

    escalate = PythonOperator(
        task_id="check_escalation",
        python_callable=check_escalation,
    )

    # task dependencies — runs in sequence
    detect >> remediate_task >> report >> escalate