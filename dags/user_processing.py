from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import json
import pandas as pd
from datetime import datetime

def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user["results"][0]
    process_user = pd.json_normalize({
        "firstaname": user["name"]["first"],
        "lastname": user["name"]["last"],
        "country": user["location"]["country"],
        "username": user["login"]["username"],
        "password": user["login"]["password"],
        "email": user["email"] })

    process_user.to_csv("/tmp/processed_user.csv", index=False, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY users FROM stdin WITH delimiter ','",
        filename="/tmp/processed_user.csv"
    )

def _see_results():
    hook = PostgresHook(postgres_conn_id="postgres")
    query = "SELECT * FROM users limit 10;"
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)

    results = cursor.fetchall()
    return results


with DAG("user_processing", start_date=datetime(2022, 7, 1), schedule_interval="@daily", catchup=False) as dag:
    
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastaname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="/api"
    )

    extract_user = SimpleHttpOperator(
        task_id="extract_user",
        http_conn_id="user_api",
        endpoint="/api",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_user = PythonOperator(
        task_id="process_user",
        python_callable=_process_user
    )

    store_user = PythonOperator(
        task_id="store_user",
        python_callable=_store_user
    )

    see_results = PythonOperator(
        task_id="see_results",
        python_callable=_see_results
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user >> see_results
