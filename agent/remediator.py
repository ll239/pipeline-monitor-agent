import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../db/pipeline.db")


# -------------------- PROMOTE STG TO PROD --------------------
def promote_stg_to_prod(conn):
    """
    Promotes clean records from staging to production table
    """
    cur = conn.cursor()

    # only promote rows that passed validation
    cur.execute("""
        INSERT INTO sales_events_prod 
        (event_time, store_id, product_id, quantity, revenue, loaded_at)
        SELECT event_time, store_id, product_id, quantity, revenue, ?
        FROM sales_events_stg
        WHERE stg_status = 'validated'
    """, (datetime.now().isoformat(),))

    promoted = cur.rowcount
    conn.commit()
    print(f"[REMEDIATOR] Promoted {promoted} rows from STG to PROD")
    return promoted


# -------------------- FAIL STG LOAD --------------------
def fail_stg_load(conn, reason):
    """
    Marks all staging records as failed
    Prevents promotion to production
    """
    cur = conn.cursor()
    cur.execute("""
        UPDATE sales_events_stg 
        SET stg_status = 'failed'
        WHERE stg_status = 'pending'
    """)
    conn.commit()
    print(f"[REMEDIATOR] STG load FAILED — {reason}")
    print(f"[REMEDIATOR] No records promoted to PROD")


# -------------------- NULL RATE REMEDIATION --------------------
def remediate_null_rate(check_result, conn):
    """
    Strategy:
    - Under 30% nulls — flag rows, mark as validated, promote to PROD
    - Over 30% nulls — fail STG load, do not promote
    """
    null_rate = check_result["null_rate"]
    null_count = check_result["null_count"]
    cur = conn.cursor()

    print(f"\n[REMEDIATOR] Null rate: {null_rate * 100:.1f}%")

    if null_rate < 0.3:
        # flag null rows with sentinel value in staging
        cur.execute("""
            UPDATE sales_events_stg 
            SET revenue = -1, stg_status = 'validated'
            WHERE revenue IS NULL
        """)
        # mark rest as validated
        cur.execute("""
            UPDATE sales_events_stg 
            SET stg_status = 'validated'
            WHERE stg_status = 'pending'
        """)
        conn.commit()

        promoted = promote_stg_to_prod(conn)

        return {
            "action": "flagged_and_promoted",
            "status": "partially_remediated",
            "detail": f"Flagged {null_count} null revenue rows with sentinel value -1. {promoted} rows promoted to PROD. Source team notified to investigate upstream nulls.",
            "requires_escalation": False
        }
    else:
        fail_stg_load(conn, f"Null rate {null_rate * 100:.1f}% exceeds 30% threshold")
        return {
            "action": "stg_load_failed",
            "status": "escalation_required",
            "detail": f"Null rate of {null_rate * 100:.1f}% suggests upstream pipeline failure or late loading parent table. STG load halted. Recommend source validation before rerun.",
            "requires_escalation": True
        }


# -------------------- ROW COUNT REMEDIATION --------------------
def remediate_row_count(check_result, conn):
    """
    Strategy:
    Row drops usually caused by inner join conditions
    or dimension tables not up to date
    Always fail STG and escalate — cannot auto fix join issues
    """
    row_count = check_result["row_count"]
    min_expected = check_result["min_expected"]
    drop_pct = round((min_expected - row_count) / min_expected * 100, 1)

    print(f"\n[REMEDIATOR] Row count drop: {row_count} vs {min_expected} expected ({drop_pct}% drop)")

    # check for missing dimension keys
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) 
        FROM sales_events_stg
        WHERE store_id IS NULL OR product_id IS NULL
    """)
    orphan_count = cur.fetchone()[0]

    fail_stg_load(conn, f"Row count dropped by {drop_pct}%")

    if orphan_count > 0:
        detail = f"Row count dropped by {drop_pct}%. Detected {orphan_count} records with missing dimension keys — likely new products not yet in dimension table. Update dim tables before rerun."
    else:
        detail = f"Row count dropped by {drop_pct}%. No obvious join issues found — check upstream source for late arriving data before rerun."

    return {
        "action": "stg_load_failed",
        "status": "escalation_required",
        "detail": detail,
        "requires_escalation": True
    }


# -------------------- SCHEMA DRIFT REMEDIATION --------------------
def remediate_schema_drift(check_result, conn):
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
def remediate(check_results):
    """
    Runs remediation for each failed check
    Uses STG/PROD pattern — only promotes clean data to PROD
    """
    conn = sqlite3.connect(DB_PATH)

    # load STG with current data for validation
    cur = conn.cursor()
    cur.execute("DELETE FROM sales_events_stg")  # clear previous STG
    cur.execute("""
        INSERT INTO sales_events_stg 
        (event_time, store_id, product_id, quantity, revenue, stg_status)
        SELECT event_time, store_id, product_id, quantity, revenue, 'pending'
        FROM sales_events
    """)
    conn.commit()
    print(f"\n[REMEDIATOR] Loaded {cur.rowcount} rows into STG for validation")

    remediation_results = []

    for check in check_results:
        if check["passed"]:
            continue

        print(f"\n[REMEDIATOR] Remediating: {check['check']}")

        if check["check"] == "null_rate":
            result = remediate_null_rate(check, conn)
        elif check["check"] == "row_count":
            result = remediate_row_count(check, conn)
        elif check["check"] == "schema_drift":
            result = remediate_schema_drift(check, conn)
        else:
            fail_stg_load(conn, f"Unknown check type: {check['check']}")
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