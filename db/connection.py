import os
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../monitor_config.yaml")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_connection():
    """
    Returns a database connection based on monitor_config.yaml
    Supports: sqlite, snowflake, postgres, bigquery
    """
    config = load_config()
    conn_config = config["connection"]
    db_type = conn_config["type"]

    if db_type == "sqlite":
        import sqlite3
        db_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            conn_config["path"].replace("db/", "")
        )
        print(f"[CONNECTION] Connected to SQLite: {db_path}")
        return sqlite3.connect(db_path)

    elif db_type == "snowflake":
        import snowflake.connector
        print(f"[CONNECTION] Connecting to Snowflake: {conn_config['account']}")
        return snowflake.connector.connect(
            account=conn_config["account"],
            user=conn_config["user"],
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=conn_config["database"],
            schema=conn_config["schema"],
            warehouse=conn_config["warehouse"],
            role=conn_config.get("role")
        )

    elif db_type == "postgres":
        import psycopg2
        print(f"[CONNECTION] Connecting to Postgres: {conn_config['host']}")
        return psycopg2.connect(
            host=conn_config["host"],
            database=conn_config["database"],
            user=conn_config["user"],
            password=os.getenv("POSTGRES_PASSWORD"),
            port=conn_config.get("port", 5432)
        )

    elif db_type == "bigquery":
        from google.cloud import bigquery
        print(f"[CONNECTION] Connecting to BigQuery: {conn_config['project']}")
        return bigquery.Client(project=conn_config["project"])

    else:
        raise ValueError(f"Unsupported database type: {db_type}. Supported: sqlite, snowflake, postgres, bigquery")


def get_table_names():
    """Returns list of all table names from config"""
    config = load_config()
    return [t["name"] for t in config["tables"]]


def get_table_config(table_name):
    """Returns config for a specific table"""
    config = load_config()
    for table in config["tables"]:
        if table["name"] == table_name:
            return table
    raise ValueError(f"Table '{table_name}' not found in monitor_config.yaml")


def get_check_config(table_name, check_name):
    """Returns config for a specific check on a specific table"""
    table_config = get_table_config(table_name)
    return table_config["checks"].get(check_name, {})


if __name__ == "__main__":
    config = load_config()
    print(f"Pipeline: {config['pipeline']['name']}")
    print(f"DB type: {config['connection']['type']}")
    print(f"Tables: {get_table_names()}")
    conn = get_connection()
    print(f"Connection successful: {conn}")