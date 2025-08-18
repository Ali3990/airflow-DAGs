from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Alan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sftp_to_postgres",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False
) as dag:

    download_file = SFTPOperator(
        task_id="download_latest",
        ssh_conn_id="MySFTPConn",  # Airflow connection for SFTP
        local_filepath="/tmp/latest_file.csv",
        remote_filepath="/remote/path/to/latest.csv",
        operation="get"
    )

    load_to_postgres = SQLExecuteQueryOperator(
        task_id="load_data",
        conn_id="DBeaverConn",
        sql="""
            COPY my_tests.sftp_data
            FROM '/tmp/latest_file.csv'
            DELIMITER ','
            CSV HEADER;
        """
    )

    download_file >> load_to_postgres