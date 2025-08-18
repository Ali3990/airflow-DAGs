from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'Alan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_with_postgres_operator_test_v02',
    default_args=default_args,
    start_date=datetime(2021, 12, 19),
    schedule=None,  # Run manually for testing
    catchup=False
) as dag:

    # Step 1: Create schema if not exists
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id='DBeaverConn',
        sql="CREATE SCHEMA IF NOT EXISTS my_tests;"
    )

    # Step 2: Create table inside the schema
    create_table = SQLExecuteQueryOperator(
        task_id='create_test_table',
        conn_id='DBeaverConn',
        sql="""
            CREATE TABLE IF NOT EXISTS my_tests.test_table (
                id SERIAL PRIMARY KEY,
                name TEXT
            );
        """
    )

    create_schema >> create_table