import sqlite3
import os
import yaml
import os

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


def run_all_checks():
    config = load_config()
    table_config = config["tables"][0]  # first table for now
    checks_config = table_config["checks"]
    table_name = table_config["name"]

    results = []

    # null rate
    if checks_config["null_rate"]["enabled"]:
        results.append(check_null_rate(
            table=table_name,
            column=checks_config["null_rate"]["column"],
            threshold=checks_config["null_rate"]["threshold"]
        ))

    # row count
    if checks_config["row_count"]["enabled"]:
        results.append(check_row_count(
            table=table_name,
            min_expected=checks_config["row_count"]["min_expected"]
        ))

    # schema drift
    if checks_config["schema_drift"]["enabled"]:
        results.append(check_schema(
            table=table_name,
            expected_columns=set(checks_config["schema_drift"]["expected_columns"])
        ))

    # duplicate check
    if checks_config["duplicate_check"]["enabled"]:
        results.append(check_duplicates(
            table=table_name,
            column=checks_config["duplicate_check"]["column"],
            threshold=checks_config["duplicate_check"]["threshold"]
        ))

    # freshness check
    if checks_config["freshness_check"]["enabled"]:
        results.append(check_freshness(
            table=table_name,
            timestamp_column=checks_config["freshness_check"]["timestamp_column"],
            max_hours=checks_config["freshness_check"]["max_hours_since_update"]
        ))

    # value range check
    if checks_config["value_range_check"]["enabled"]:
        results.append(check_value_range(
            table=table_name,
            column=checks_config["value_range_check"]["column"],
            min_val=checks_config["value_range_check"]["min_value"],
            max_val=checks_config["value_range_check"]["max_value"]
        ))

    print("\n--- Pipeline Health Checks ---")
    for r in results:
        status = "✅ PASSED" if r["passed"] else "❌ FAILED"
        print(f"{status} | {r['check']}")

    return results


if __name__ == "__main__":
    run_all_checks()