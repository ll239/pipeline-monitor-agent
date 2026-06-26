import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline.db")

def create_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            store_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            revenue REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT,
            rows_processed INTEGER,
            status TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_events_stg (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            store_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            revenue REAL,
            stg_status TEXT DEFAULT 'pending'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_events_prod (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            store_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            revenue REAL,
            loaded_at TEXT
        )
    """)

    conn.commit()
    conn.close()

def seed_normal_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    base_time = datetime.now() - timedelta(days=7)

    for i in range(500):
        cur.execute("""
            INSERT INTO sales_events (event_time, store_id, product_id, quantity, revenue)
            VALUES (?, ?, ?, ?, ?)
        """, (
            (base_time + timedelta(hours=i)).isoformat(),
            random.randint(1, 20),
            random.randint(1, 100),
            random.randint(1, 10),
            round(random.uniform(10, 500), 2)
        ))

    conn.commit()
    conn.close()
    print("Seeded 500 normal rows")

def inject_failure(failure_type="null_revenue"):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if failure_type == "null_revenue":
        cur.execute("UPDATE sales_events SET revenue = NULL WHERE id % 5 = 0")
        print("Injected: null revenue values")

    elif failure_type == "row_drop":
        cur.execute("DELETE FROM sales_events WHERE id > 400")
        print("Injected: row count drop")

    elif failure_type == "schema_drift":
        cur.execute("ALTER TABLE sales_events ADD COLUMN discount REAL")
        print("Injected: schema drift (new column added)")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_db()
    seed_normal_data()
    inject_failure("null_revenue")
print("DB Path:", DB_PATH)