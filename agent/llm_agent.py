import anthropic
import json
import os
from dotenv import load_dotenv

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

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
        model="claude-sonnet-4-5",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )

    return message.content[0].text

if __name__ == "__main__":
    from detector import run_all_checks
    results = run_all_checks()
    print("\n--- LLM Agent Analysis ---")
    analysis = analyze_failures(results)
    print(analysis)