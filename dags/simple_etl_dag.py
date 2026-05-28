import json
import os
import subprocess
from datetime import timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
RAW_DATA_PATH = "/opt/airflow/include/raw_users.json"
SNOWFLAKE_DATABASE = "DEMO_DB"
RAW_SCHEMA = "RAW"
RAW_USERS_TABLE = f"{SNOWFLAKE_DATABASE}.{RAW_SCHEMA}.raw_users"

with DAG(
    dag_id="simple_etl_with_dbt",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["example", "dbt"],
) as dag:

    @task
    def extract_data() -> str:
        url = "https://jsonplaceholder.typicode.com/users"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
        with open(RAW_DATA_PATH, "w", encoding="utf-8") as f:
            json.dump(response.json(), f, indent=2)
        return RAW_DATA_PATH

    @task
    def load_raw_data(file_path: str) -> None:
        with open(file_path, "r", encoding="utf-8") as f:
            users = json.load(f)

        snowflake = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = snowflake.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RAW_USERS_TABLE} (
                id INTEGER,
                name TEXT,
                username TEXT,
                email TEXT,
                city TEXT,
                phone TEXT,
                website TEXT,
                company TEXT
            )
            """
        )
        cursor.execute(f"TRUNCATE TABLE {RAW_USERS_TABLE}")

        insert_sql = f"""
            INSERT INTO {RAW_USERS_TABLE} (id, name, username, email, city, phone, website, company)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for user in users:
            cursor.execute(
                insert_sql,
                [
                    user.get("id"),
                    user.get("name"),
                    user.get("username"),
                    user.get("email"),
                    user.get("address", {}).get("city"),
                    user.get("phone"),
                    user.get("website"),
                    user.get("company", {}).get("name"),
                ],
            )

        conn.commit()
        cursor.close()
        conn.close()

    @task
    def run_dbt() -> None:
        subprocess.run(["dbt", "run"], cwd=DBT_PROJECT_DIR, check=True)

    @task
    def run_dbt_tests() -> None:
        subprocess.run(["dbt", "test"], cwd=DBT_PROJECT_DIR, check=True)

    raw_file = extract_data()
    load_task = load_raw_data(raw_file)
    dbt_run_task = run_dbt()
    dbt_test_task = run_dbt_tests()

    load_task >> dbt_run_task >> dbt_test_task
