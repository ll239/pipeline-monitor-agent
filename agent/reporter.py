import json
from datetime import datetime
from anthropic import Anthropic
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db"))

from connection import load_config, get_connection

load_dotenv()

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs/incident_reports.json")
anthropic_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
config = load_config()

def generate_report(check_results, remediation_results):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

    failed_checks = [c for c in check_results if not c["passed"]]
    escalations = [r for r in remediation_results if r["requires_escalation"]]

    # use LLM to write human readable summary
    if remediation_results:
        prompt = f"""You are a data engineering incident reporter.
Write a concise 3 sentence incident summary for a data team based on these findings.
Be specific, use the numbers provided, and end with a clear recommended action.

Failed checks: {json.dumps(failed_checks, indent=2)}
Remediation actions: {json.dumps(remediation_results, indent=2)}"""

        response = anthropic_client.messages.create(
            model = config.get("llm", {}).get("model", "claude-sonnet-4-6"),
            max_tokens = config.get("llm", {}).get("max_tokens", 1000),
            messages=[{"role": "user", "content": prompt}],
        )
        llm_summary = response.content[0].text.strip()
    else:
        llm_summary = "All pipeline checks passed. No action required."

    report = {
        "report_id": f"INC-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "timestamp": datetime.now().isoformat(),
        "status": "escalation_required" if escalations else "auto_remediated",
        "summary": {
            "total_checks": len(check_results),
            "failed_checks": len(failed_checks),
            "auto_remediated": len(remediation_results) - len(escalations),
            "requires_escalation": len(escalations)
        },
        "llm_summary": llm_summary,
        "failed_checks": failed_checks,
        "remediation_actions": remediation_results,
        "escalations": escalations,
        "recommendation": (
            "Pipeline requires immediate attention. See escalations section."
            if escalations else
            "All issues auto remediated. Monitor for recurrence."
        )
    }

    # save to log file
    reports = []
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, "r") as f:
            try:
                reports = json.load(f)
            except:
                reports = []

    reports.append(report)
    with open(LOG_PATH, "w") as f:
        json.dump(reports, f, indent=2)

    print(f"\n{'='*50}")
    print(f"INCIDENT REPORT — {report['report_id']}")
    print(f"{'='*50}")
    print(f"Status:           {report['status']}")
    print(f"Failed checks:    {report['summary']['failed_checks']}")
    print(f"Auto remediated:  {report['summary']['auto_remediated']}")
    print(f"Needs escalation: {report['summary']['requires_escalation']}")
    print(f"\nLLM Summary:\n{llm_summary}")
    print(f"\nRecommendation:   {report['recommendation']}")
    print(f"Report saved to:  {LOG_PATH}")

    return report


if __name__ == "__main__":
    from detector import run_all_checks
    from remediator import remediate

    print("Running full pipeline monitor...")
    check_results = run_all_checks()
    remediation_results = remediate(check_results)
    report = generate_report(check_results, remediation_results)