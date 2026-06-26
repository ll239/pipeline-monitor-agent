import sqlite3
from datetime import datetime
import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db"))

from connection import get_connection, get_table_config, get_check_config

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db/pipeline.db")


# -------------------- PROMOTE STG TO PROD --------------------
def promote_stg_to_prod(conn, stg_table="sales_events_stg", prod_table="sales_events_prod"):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {prod_table}
        (event_time, store_id, product_id, quantity, revenue, loaded_at)
        SELECT event_time, store_id, product_id, quantity, revenue, ?
        FROM {stg_table}
        WHERE stg_status = 'validated'
    """, (datetime.now().isoformat(),))
    promoted = cur.rowcount
    conn.commit()
    print(f"[REMEDIATOR] Promoted {promoted} rows from {stg_table} to {prod_table}")
    return promoted

def fail_stg_load(conn, reason, stg_table="sales_events_stg"):
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE {stg_table} 
        SET stg_status = 'failed'
        WHERE stg_status = 'pending'
    """)
    conn.commit()
    print(f"[REMEDIATOR] STG load FAILED — {reason}")
    print(f"[REMEDIATOR] No records promoted to PROD")

# -------------------- NULL RATE REMEDIATION --------------------
def remediate_null_rate(check_result, conn, escalation_threshold=0.3,
                         auto_remediate=True, stg_table="sales_events_stg",
                         prod_table="sales_events_prod"):
    null_rate = check_result["null_rate"]
    null_count = check_result["null_count"]
    cur = conn.cursor()

    print(f"\n[REMEDIATOR] Null rate: {null_rate*100:.1f}%")

    if not auto_remediate:
        fail_stg_load(conn, "Auto remediation disabled in config", stg_table)
        return {
            "action": "stg_load_failed",
            "status": "escalation_required",
            "detail": "Auto remediation disabled in config. Manual review required.",
            "requires_escalation": True
        }

    if null_rate < escalation_threshold:
        cur.execute(f"""
            UPDATE {stg_table} 
            SET revenue = -1, stg_status = 'validated'
            WHERE revenue IS NULL
        """)
        cur.execute(f"""
            UPDATE {stg_table} 
            SET stg_status = 'validated'
            WHERE stg_status = 'pending'
        """)
        conn.commit()
        promoted = promote_stg_to_prod(conn, stg_table, prod_table)
        return {
            "action": "flagged_and_promoted",
            "status": "partially_remediated",
            "detail": f"Flagged {null_count} null rows with sentinel -1. {promoted} rows promoted to PROD.",
            "requires_escalation": False
        }
    else:
        fail_stg_load(conn, f"Null rate {null_rate*100:.1f}% exceeds threshold", stg_table)
        return {
            "action": "stg_load_failed",
            "status": "escalation_required",
            "detail": f"Null rate {null_rate*100:.1f}% exceeds escalation threshold {escalation_threshold*100:.0f}%.",
            "requires_escalation": True
        }
# -------------------- ROW COUNT REMEDIATION --------------------
def remediate_row_count(check_result, conn, stg_table="sales_events_stg"):
    row_count = check_result["row_count"]
    min_expected = check_result["min_expected"]
    drop_pct = round((min_expected - row_count) / min_expected * 100, 1)

    print(f"\n[REMEDIATOR] Row count drop: {row_count} vs {min_expected} expected ({drop_pct}% drop)")

    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) 
        FROM {stg_table}
        WHERE store_id IS NULL OR product_id IS NULL
    """)
    orphan_count = cur.fetchone()[0]

    fail_stg_load(conn, f"Row count dropped by {drop_pct}%", stg_table)

    if orphan_count > 0:
        detail = f"Row count dropped by {drop_pct}%. Detected {orphan_count} records with missing dimension keys — update dim tables before rerun."
    else:
        detail = f"Row count dropped by {drop_pct}%. Check upstream source for late arriving data before rerun."

    return {
        "action": "stg_load_failed",
        "status": "escalation_required",
        "detail": detail,
        "requires_escalation": True
    }

# -------------------- SCHEMA DRIFT REMEDIATION --------------------
def remediate_schema_drift(check_result, conn, stg_table="sales_events_stg"):
    """
    Strategy:
    Fail STG load immediately
    Never promote unexpected schema to PROD
    Alert source team
    """
    unexpected = check_result["unexpected_columns"]
    missing = check_result["missing_columns"]

    print(f"\n[REMEDIATOR] Schema drift detected!")
    print(f"  Unexpected: {unexpected}")
    print(f"  Missing: {missing}")

    fail_stg_load(conn, "Schema drift detected")

    detail = ""
    if unexpected:
        detail += f"Unexpected columns: {unexpected}. "
    if missing:
        detail += f"Missing columns: {missing}. "
    detail += "STG load halted. Contact source team before rerun."

    return {
        "action": "stg_load_failed",
        "status": "escalation_required",
        "detail": detail,
        "requires_escalation": True
    }


# -------------------- MAIN REMEDIATION FUNCTION --------------------
def remediate(check_results, table_name="sales_events"):
    conn = get_connection()
    table_config = get_table_config(table_name)
    checks_config = table_config["checks"]

    # get staging and prod table names from config
    stg_table = table_config.get("staging_table", f"{table_name}_stg")
    prod_table = table_config.get("production_table", f"{table_name}_prod")

    cur = conn.cursor()
    cur.execute(f"DELETE FROM {stg_table}")
    cur.execute(f"""
        INSERT INTO {stg_table} 
        (event_time, store_id, product_id, quantity, revenue, stg_status)
        SELECT event_time, store_id, product_id, quantity, revenue, 'pending'
        FROM {table_name}
    """)
    conn.commit()
    print(f"\n[REMEDIATOR] Loaded {cur.rowcount} rows into {stg_table}")

    remediation_results = []

    for check in check_results:
        if check["passed"]:
            continue

        print(f"\n[REMEDIATOR] Remediating: {check['check']}")

        if check["check"] == "null_rate":
            # read thresholds from config instead of hardcoding
            null_config = checks_config.get("null_rate", {})
            escalation_threshold = null_config.get("escalation_threshold", 0.3)
            auto_remediate = null_config.get("auto_remediate", True)
            result = remediate_null_rate(
                check, conn,
                escalation_threshold=escalation_threshold,
                auto_remediate=auto_remediate,
                stg_table=stg_table,
                prod_table=prod_table
            )

        elif check["check"] == "row_count":
            auto_remediate = checks_config.get("row_count", {}).get("auto_remediate", False)
            result = remediate_row_count(check, conn, stg_table=stg_table)

        elif check["check"] == "schema_drift":
            result = remediate_schema_drift(check, conn, stg_table=stg_table)

        else:
            fail_stg_load(conn, f"Unknown check: {check['check']}", stg_table=stg_table)
            result = {
                "action": "stg_load_failed",
                "status": "escalation_required",
                "detail": f"No remediation strategy for: {check['check']}",
                "requires_escalation": True
            }

        result["check"] = check["check"]
        result["timestamp"] = datetime.now().isoformat()
        remediation_results.append(result)

        print(f"  Action: {result['action']}")
        print(f"  Status: {result['status']}")
        print(f"  Detail: {result['detail']}")

    conn.close()
    return remediation_results


if __name__ == "__main__":
    from detector import run_all_checks

    print("Running checks...")
    check_results = run_all_checks()

    print("\n" + "=" * 50)
    print("Running remediator with STG/PROD pattern...")
    print("=" * 50)
    remediation_results = remediate(check_results)

    print("\n" + "=" * 50)
    print("Remediation Summary")
    print("=" * 50)
    needs_escalation = [r for r in remediation_results if r["requires_escalation"]]
    print(f"Total issues:      {len(remediation_results)}")
    print(f"Auto remediated:   {len(remediation_results) - len(needs_escalation)}")
    print(f"Needs escalation:  {len(needs_escalation)}")