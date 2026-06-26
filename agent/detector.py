import sqlite3

import yaml
import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db"))

from connection import get_connection, get_table_config, get_check_config, get_table_names

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../monitor_config.yaml")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db/pipeline.db")

def check_null_rate(table="sales_events", column="revenue", threshold=0.1):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
    nulls = cur.fetchone()[0]

    conn.close()

    null_rate = nulls / total if total > 0 else 0
    return {
        "check": "null_rate",
        "table": table,
        "column": column,
        "total_rows": total,
        "null_count": nulls,
        "null_rate": round(null_rate, 4),
        "passed": null_rate <= threshold
    }

def check_row_count(table="sales_events", min_expected=400):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]

    conn.close()

    return {
        "check": "row_count",
        "table": table,
        "row_count": count,
        "min_expected": min_expected,
        "passed": count >= min_expected
    }

def check_schema(table="sales_events", expected_columns=None):
    if expected_columns is None:
        expected_columns = {"id", "event_time", "store_id", "product_id", "quantity", "revenue"}

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f"PRAGMA table_info({table})")
    actual_columns = {row[1] for row in cur.fetchall()}

    conn.close()

    unexpected = actual_columns - expected_columns
    missing = expected_columns - actual_columns

    return {
        "check": "schema_drift",
        "table": table,
        "expected_columns": list(expected_columns),
        "actual_columns": list(actual_columns),
        "unexpected_columns": list(unexpected),
        "missing_columns": list(missing),
        "passed": len(unexpected) == 0 and len(missing) == 0
    }
def check_duplicates(table="sales_events", column="id", threshold=0.0):
    """
    Checks for duplicate values in a column
    Zero tolerance by default — any duplicate is a failure
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE {column} IN (
            SELECT {column} FROM {table}
            GROUP BY {column}
            HAVING COUNT(*) > 1
        )
    """)
    duplicate_count = cur.fetchone()[0]
    conn.close()

    duplicate_rate = duplicate_count / total if total > 0 else 0

    return {
        "check": "duplicate_check",
        "table": table,
        "column": column,
        "total_rows": total,
        "duplicate_count": duplicate_count,
        "duplicate_rate": round(duplicate_rate, 4),
        "passed": duplicate_rate <= threshold
    }


def check_freshness(table="sales_events", timestamp_column="event_time", max_hours=24):
    """
    Checks if data is fresh enough
    Fails if most recent record is older than max_hours
    """
    from datetime import datetime, timedelta

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f"""
        SELECT MAX({timestamp_column}) FROM {table}
    """)
    latest = cur.fetchone()[0]
    conn.close()

    if not latest:
        return {
            "check": "freshness_check",
            "table": table,
            "timestamp_column": timestamp_column,
            "latest_record": None,
            "max_hours": max_hours,
            "passed": False,
            "reason": "No records found in table"
        }

    latest_dt = datetime.fromisoformat(latest)
    age_hours = (datetime.now() - latest_dt).total_seconds() / 3600
    is_fresh = age_hours <= max_hours

    return {
        "check": "freshness_check",
        "table": table,
        "timestamp_column": timestamp_column,
        "latest_record": latest,
        "age_hours": round(age_hours, 2),
        "max_hours": max_hours,
        "passed": is_fresh,
        "reason": f"Data is {round(age_hours, 1)} hours old" if not is_fresh else "Data is fresh"
    }


def check_value_range(table="sales_events", column="revenue", min_val=0, max_val=999999):
    """
    Checks if values in a column fall within expected range
    Flags rows outside the range
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE {column} IS NOT NULL
        AND ({column} < ? OR {column} > ?)
    """, (min_val, max_val))
    out_of_range = cur.fetchone()[0]
    conn.close()

    return {
        "check": "value_range_check",
        "table": table,
        "column": column,
        "total_rows": total,
        "out_of_range_count": out_of_range,
        "min_value": min_val,
        "max_value": max_val,
        "passed": out_of_range == 0
    }


def run_all_checks(table_name=None):
    # if no table specified, check all tables in config
    if table_name:
        tables = [get_table_config(table_name)]
    else:
        tables = [get_table_config(t) for t in get_table_names()]

    all_results = []

    for table_config in tables:
        tname = table_config["name"]
        checks = table_config["checks"]

        print(f"\n--- Checking table: {tname} ---")

        if checks.get("null_rate", {}).get("enabled"):
            all_results.append(check_null_rate(
                table=tname,
                column=checks["null_rate"]["column"],
                threshold=checks["null_rate"]["threshold"]
            ))

        if checks.get("row_count", {}).get("enabled"):
            all_results.append(check_row_count(
                table=tname,
                min_expected=checks["row_count"]["min_expected"]
            ))

        if checks.get("schema_drift", {}).get("enabled"):
            all_results.append(check_schema(
                table=tname,
                expected_columns=set(checks["schema_drift"]["expected_columns"])
            ))

        if checks.get("duplicate_check", {}).get("enabled"):
            all_results.append(check_duplicates(
                table=tname,
                column=checks["duplicate_check"]["column"],
                threshold=checks["duplicate_check"]["threshold"]
            ))

        if checks.get("freshness_check", {}).get("enabled"):
            all_results.append(check_freshness(
                table=tname,
                timestamp_column=checks["freshness_check"]["timestamp_column"],
                max_hours=checks["freshness_check"]["max_hours_since_update"]
            ))

        if checks.get("value_range_check", {}).get("enabled"):
            all_results.append(check_value_range(
                table=tname,
                column=checks["value_range_check"]["column"],
                min_val=checks["value_range_check"]["min_value"],
                max_val=checks["value_range_check"]["max_value"]
            ))

    print("\n--- Pipeline Health Checks ---")
    for r in all_results:
        status = "✅ PASSED" if r["passed"] else "❌ FAILED"
        print(f"{status} | {r['check']} | {r.get('table', '')}")

    return all_results

if __name__ == "__main__":
    run_all_checks()