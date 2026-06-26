import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../agent"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../db"))

from detector import run_all_checks
from remediator import remediate
from reporter import generate_report
from pipeline_sim import inject_failure, create_db, seed_normal_data
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db/pipeline.db")

# -------------------- EVAL SCENARIOS --------------------
SCENARIOS = [
    {
        "name": "Null Revenue Injection",
        "description": "20% null values injected into revenue column",
        "failure_type": "null_revenue",
        "expected_check_to_fail": "null_rate",
        "expected_action": "flagged_and_promoted",
        "expected_escalation": False
    },
    {
        "name": "Row Count Drop",
        "description": "Records deleted to simulate row drop below threshold",
        "failure_type": "row_drop",
        "expected_check_to_fail": "row_count",
        "expected_action": "stg_load_failed",
        "expected_escalation": True
    },
    {
        "name": "Schema Drift",
        "description": "Unexpected column added to simulate schema change",
        "failure_type": "schema_drift",
        "expected_check_to_fail": "schema_drift",
        "expected_action": "stg_load_failed",
        "expected_escalation": True
    }
]

# -------------------- RESET DB --------------------
def reset_db():
    """Resets DB to clean state before each scenario"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM sales_events")
    cur.execute("DELETE FROM sales_events_stg")
    cur.execute("DELETE FROM sales_events_prod")
    conn.commit()
    conn.close()
    seed_normal_data()

# -------------------- RUN ONE SCENARIO --------------------
def run_scenario(scenario):
    print(f"\n{'='*60}")
    print(f"SCENARIO: {scenario['name']}")
    print(f"Description: {scenario['description']}")
    print('='*60)

    # reset and inject failure
    reset_db()
    inject_failure(scenario["failure_type"])

    # run full pipeline
    check_results = run_all_checks()
    remediation_results = remediate(check_results)
    report = generate_report(check_results, remediation_results)

    # score the scenario
    score = evaluate_scenario(scenario, check_results, remediation_results)
    return score

# -------------------- EVALUATE SCENARIO --------------------
def evaluate_scenario(scenario, check_results, remediation_results):
    scores = {}

    # check 1 — did the right check fail?
    expected_fail = scenario["expected_check_to_fail"]
    failed_checks = [c["check"] for c in check_results if not c["passed"]]
    scores["correct_detection"] = expected_fail in failed_checks

    # check 2 — was the right action taken?
    expected_action = scenario["expected_action"]
    actions_taken = [r["action"] for r in remediation_results]
    scores["correct_action"] = expected_action in actions_taken

    # check 3 — was escalation handled correctly?
    expected_escalation = scenario["expected_escalation"]
    actual_escalation = any(r["requires_escalation"] for r in remediation_results)
    scores["correct_escalation"] = expected_escalation == actual_escalation

    # overall pass
    scores["passed"] = all(scores.values())

    print(f"\n--- Scenario Score ---")
    print(f"Correct detection:  {'✅' if scores['correct_detection'] else '❌'}")
    print(f"Correct action:     {'✅' if scores['correct_action'] else '❌'}")
    print(f"Correct escalation: {'✅' if scores['correct_escalation'] else '❌'}")
    print(f"Overall:            {'✅ PASSED' if scores['passed'] else '❌ FAILED'}")

    return scores

# -------------------- RUN ALL SCENARIOS --------------------
def run_all_scenarios():
    print("\n" + "="*60)
    print("PIPELINE MONITOR AGENT — EVAL HARNESS")
    print("="*60)

    all_scores = []
    for scenario in SCENARIOS:
        score = run_scenario(scenario)
        score["scenario"] = scenario["name"]
        all_scores.append(score)

    # final summary
    passed = sum(1 for s in all_scores if s["passed"])
    total = len(all_scores)

    print(f"\n{'='*60}")
    print(f"EVAL SUMMARY")
    print(f"{'='*60}")
    for s in all_scores:
        status = "✅ PASSED" if s["passed"] else "❌ FAILED"
        print(f"{status} | {s['scenario']}")

    print(f"\nOverall score: {passed}/{total} scenarios passed")
    print(f"Agent reliability: {round(passed/total*100)}%")

    return all_scores


if __name__ == "__main__":
    run_all_scenarios()