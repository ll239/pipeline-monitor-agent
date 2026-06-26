import anthropic
import json
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db"))

from connection import load_config, get_connection

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
config = load_config()
def analyze_failures(check_results: list) -> str:
    failed_checks = [r for r in check_results if not r["passed"]]

    if not failed_checks:
        return "All checks passed. No action needed."

    prompt = f"""You are a data pipeline monitoring agent. You have detected the following data quality failures:

{json.dumps(failed_checks, indent=2)}

For each failure:
1. Explain what the issue is in plain terms
2. Identify the likely root cause
3. Suggest a specific remediation action
4. Assess the severity (LOW / MEDIUM / HIGH)

Be concise and practical. Respond as if you are writing an incident report for a data engineering team."""

    message = client.messages.create(
        model=config.get("llm", {}).get("model", "claude-sonnet-4-6"),
        max_tokens=config.get("llm", {}).get("max_tokens", 1000),
        messages=[{"role": "user", "content": prompt}]
    )

    return message.content[0].text

if __name__ == "__main__":
    from detector import run_all_checks
    results = run_all_checks()
    print("\n--- LLM Agent Analysis ---")
    analysis = analyze_failures(results)
    print(analysis)