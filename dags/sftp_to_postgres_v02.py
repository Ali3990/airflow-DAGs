from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import re
import zipfile
import pandas as pd


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

LOCAL_DOWNLOAD_DIR = "/tmp/sftp_downloads"

@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sftp", "postgres", "taskflow"],
)
def sftp_to_postgres_v03():

    @task()
    def ensure_table_exists():
        hook = PostgresHook(postgres_conn_id="DBeaver_conn")
        result = hook.get_first("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'sftp_imports'
            );
        """)
        if not result[0]:
            print("Table does not exist, creating...")
        else:
            print("Table already exists, proceeding...")

    @task()
    def sftp_connection_check():
        hook = SFTPHook(ssh_conn_id="sftp_conn")
        conn = hook.get_conn()
        conn.listdir(".")
        print("✅ SFTP connection successful")

    @task()
    def list_files_in_root():
        hook = SFTPHook(ssh_conn_id="sftp_conn")
        files = hook.get_conn().listdir(".")
        print("📂 Files in SFTP root directory:", files)
        return files

    @task()
    def download_and_upload(files: list):
        if not files:
            print("⚠️ No files found in SFTP root")
            return

        # Match filenames like *_YYYYMMDD.zip and capture YYYYMM
        month_pattern = re.compile(r".*_(\d{6})\d{2}\.zip$")
        available_months = [month_pattern.match(f).group(1) for f in files if month_pattern.match(f)]
        if not available_months:
            print("⚠️ No files matching monthly pattern found")
            return

        # Get the latest month
        latest_month = max(available_months)
        print(f"🎯 Latest month detected: {latest_month}")
        target_pattern = re.compile(rf".*_{latest_month}\d{{2}}\.zip$")

        hook = SFTPHook(ssh_conn_id="sftp_conn")
        os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
        pg_hook = PostgresHook(postgres_conn_id="DBeaver_conn")

        # Loop over *all* zips that match the latest month
        for f in files:
            if not target_pattern.match(f):
                continue

            local_path = os.path.join(LOCAL_DOWNLOAD_DIR, f)
            hook.retrieve_file(remote_full_path=f, local_full_path=local_path)
            print(f"✅ Downloaded {f} → {local_path}")

            try:
                with zipfile.ZipFile(local_path, "r") as zip_ref:
                    zip_ref.extractall(LOCAL_DOWNLOAD_DIR)
                    for txt_file in zip_ref.namelist():
                        if txt_file.endswith(".txt"):
                            txt_path = os.path.join(LOCAL_DOWNLOAD_DIR, txt_file)
                            df = pd.read_csv(txt_path, sep="|", dtype=str)
                            if df.empty:
                                print(f"⚠️ {txt_file} is empty, skipping")
                                continue

                            # ✅ Derive table name safely from file name
                            raw_name = os.path.basename(txt_file)
                            table_name = re.sub(r"\.txt$", "", raw_name, flags=re.IGNORECASE)
                            table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
                            table_name = table_name.lower()

                            # Truncate to Postgres max identifier length (63)
                            table_name = table_name[:63]

                            print(f"📌 Creating/loading table: {table_name}")

                            # Connect to Postgres
                            conn = pg_hook.get_conn()
                            cur = conn.cursor()

                            # Drop + recreate (optional: remove DROP if you only want append)
                            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
                            columns_with_types = ", ".join([f'"{col}" VARCHAR(50)' for col in df.columns])
                            cur.execute(f'CREATE TABLE "{table_name}" ({columns_with_types});')
                            conn.commit()

                            # Convert DataFrame → CSV in memory
                            from io import StringIO
                            csv_buffer = StringIO()
                            df.to_csv(csv_buffer, index=False, header=False)
                            csv_buffer.seek(0)

                            # Bulk load with COPY
                            cur.copy_expert(
                                f'COPY "{table_name}" FROM STDIN WITH CSV',
                                csv_buffer
                            )

                            conn.commit()
                            cur.close()
                            conn.close()
                            print(f"✅ Bulk uploaded {len(df)} rows into {table_name}")
            except zipfile.BadZipFile as e:
                    print(f"❌ Failed to extract {f}: {e}")
 

    # DAG task flow
    table_check = ensure_table_exists()
    conn_check = sftp_connection_check()
    files = list_files_in_root()
    download_upload = download_and_upload(files)

    table_check >> conn_check >> files >> download_upload

dag = sftp_to_postgres_v03()
