from __future__ import annotations

# [START synthetic_data_dag]
# [START import_module]

import json
import os
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

# [END import_module]

S3_BUCKET = os.getenv("S3_BUCKET", "mlops-data-nr")
S3_BUCKET_PREFIX = os.getenv("S3_BUCKET_PREFIX", "fraud-data-auto/")


# [START instantiate_dag]
with DAG(
    "synthetic_data_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 0},
    # [END default_args]
    description="Generate synthetic transactions",
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2023, 2, 9, tz="UTC"),
    catchup=True,
    tags=["mlops", "anti-fraud"],
    max_active_runs=1
) as dag:
    # [END instantiate_dag]

    # [START generate_function]
    def generate_synthetic_txs(**kwargs):
        import logging
        import math
        import os
        import pandas as pd
        import random
        import time
        from datetime import datetime as dt
        from datetime import timedelta
        from pathlib import Path

        LOCAL_DIR = os.getenv("LOCAL_DIR", "/home/airflow/dags/synthetic-tx-data/")

        dt_format = "%Y-%m-%d %H:%M:%S"

        tx_per_minute = 1050
        dt_from_str = f"{kwargs['ds']} 00:00:00"
        dt_to_str = f"{kwargs['ds']} 23:59:59"

        dt_from = dt.strptime(dt_from_str, dt_format)
        dt_to = dt.strptime(dt_to_str, dt_format)

        tx_id_random_shift = 60 * 60 * 24 * 365 * 5
        dt_tx_begin = dt(2019, 8, 22, 0, 0)

        def generate_tx_id(tx_dt: dt) -> int:

            return round(time.mktime(tx_dt.timetuple()) + random.randint(0, tx_id_random_shift))

        def generate_tx_customer_id() -> int:
            return random.randint(0, 999999)

        def generate_tx_terminal_id() -> int:
            return random.randint(0, 9999)

        def generate_tx_amount() -> float:
            return round(random.uniform(1.0, 700.0), 2)

        def generate_tx_time_seconds(tx_dt: dt) -> int:
            dt_delta = tx_dt - dt_tx_begin
            tx_time_seconds = math.ceil(dt_delta.total_seconds())

            return tx_time_seconds

        def generate_tx_time_days(tx_dt: dt) -> int:
            dt_delta = tx_dt - dt_tx_begin
            tx_time_days = math.floor(dt_delta.total_seconds() / 60 / 60 / 24)

            return tx_time_days

        def generate_tx_fraud_type() -> dict:
            """
                tx_fraud    tx_fraud_scenario 	group_cnt 	group_perc 	group_perc100 	group_perc100_cumsum
            0 	0 	0 	4672230 	93.44460 	9344.460 	9344.460
            1 	1 	1 	2633 		0.05266 	5.266 		9349.726
            2 	1 	2 	318348 		6.36696 	636.696 	9986.422
            3 	1 	3 	6789 		0.13578 	13.578 		10000.000
            """
            rand_val = random.uniform(0.0, 10000.0)
            if rand_val <= 9344.460:
                return {"tx_fraud": 0, "tx_fraud_scenario": 0}
            elif rand_val <= 9349.726:
                return {"tx_fraud": 1, "tx_fraud_scenario": 1}
            elif rand_val <= 9986.422:
                return {"tx_fraud": 1, "tx_fraud_scenario": 2}
            else:
                return {"tx_fraud": 1, "tx_fraud_scenario": 3}

        def generate_tx_in_period(dt_from: dt, dt_to: dt) -> dict:
            dt_delta = dt_to - dt_from
            dt_delta_seconds = round(dt_delta.total_seconds())
            tx_dt = dt_from + timedelta(seconds=random.randint(0, dt_delta_seconds))

            tx_fraud_type = generate_tx_fraud_type()

            return {
                "transaction_id": generate_tx_id(tx_dt),
                "tx_datetime": tx_dt,
                "customer_id": generate_tx_customer_id(),
                "terminal_id": generate_tx_terminal_id(),
                "tx_amount": generate_tx_amount(),
                "tx_time_seconds": generate_tx_time_seconds(tx_dt),
                "tx_time_days": generate_tx_time_days(tx_dt),
                "tx_fraud": tx_fraud_type["tx_fraud"],
                "tx_fraud_scenario": tx_fraud_type["tx_fraud_scenario"],
            }

        def generate_txs_in_period(dt_from: dt, dt_to, tx_per_minute: int):
            dt_delta = dt_to - dt_from
            dt_delta_minutes = dt_delta.total_seconds() / 60
            minutes = math.ceil(dt_delta_minutes)

            for minute in range(minutes):
                for i in range(tx_per_minute):
                    yield generate_tx_in_period(dt_from + timedelta(minutes=minute),
                                                dt_from + timedelta(minutes=minute + 1))

        logging.info(f"Generating transactions from {dt_from_str} to {dt_to_str}")
        tx_generator = generate_txs_in_period(dt_from, dt_to, tx_per_minute)
        rows_list = []
        tx = next(tx_generator, False)
        while tx:
            rows_list.append(tx)
            tx = next(tx_generator, False)
        logging.info(f"Generated {len(rows_list)} transactions")
        df = pd.DataFrame(rows_list)

        saved_dt_format = "%Y-%m-%d-%H-%M-%S"
        saved_dt_from = dt_from.strftime(saved_dt_format)
        saved_dt_to = dt_to.strftime(saved_dt_format)

        Path(LOCAL_DIR).mkdir(parents=True, exist_ok=True)
        local_filename = f"transactions__{saved_dt_from}__{saved_dt_to}.parquet"
        local_filepath = Path(LOCAL_DIR + local_filename)
        df.to_parquet(local_filepath)
        logging.info(f"Saved to {local_filepath}")

        ti = kwargs["ti"]
        ti.xcom_push(key="local_filename", value=local_filename)
        ti.xcom_push(key="local_filepath", value=str(local_filepath))
    # [END generate_function]

    # [START upload_s3_function]
    def upload_to_s3(**kwargs):
        import boto3
        import logging
        from airflow.hooks.base import BaseHook
        from botocore.exceptions import ClientError

        ti = kwargs["ti"]
        local_filename = ti.xcom_pull(task_ids="generate_synthetic_txs", key="local_filename")
        local_filepath = ti.xcom_pull(task_ids="generate_synthetic_txs", key="local_filepath")
        object_name = S3_BUCKET_PREFIX + local_filename

        logging.info(f"Uploading {local_filepath} to S3 bucket {S3_BUCKET} with key {object_name}")

        s3_connection = BaseHook.get_connection("yandex_cloud_s3")
        s3_access_key = s3_connection.login
        s3_secret_key = s3_connection.password
        logging.info(f"s3_connection_extra = {s3_connection.get_extra()}")
        s3_connection_extra = json.loads(s3_connection.get_extra())
        s3_region_name = s3_connection_extra["region_name"]
        s3_endpoint_url = s3_connection_extra["endpoint_url"]

        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=s3_region_name,
        )
        try:
            s3_client.upload_file(local_filepath, S3_BUCKET, object_name)
            logging.info(f"Uploaded {local_filepath} to S3 bucket {S3_BUCKET} with key {object_name}")
        except ClientError as e:
            logging.info(f"Error while uploading {local_filepath} to S3 bucket {S3_BUCKET} with key {object_name}")
            logging.error(e)
            raise
    # [END upload_s3_function]

    # [START upload_hdfs_function]
    def upload_to_hdfs(**kwargs):
        import logging
        import os
        import pyhdfs
        from airflow.hooks.base import BaseHook

        ti = kwargs["ti"]
        local_filename = ti.xcom_pull(task_ids="generate_synthetic_txs", key="local_filename")
        local_filepath = ti.xcom_pull(task_ids="generate_synthetic_txs", key="local_filepath")
        HDFS_FOLDER = os.getenv("HDFS_FOLDER", "/fraud-data-auto/")
        hdfs_filepath = HDFS_FOLDER + local_filename

        logging.info(f"Uploading {local_filepath} to HDFS {hdfs_filepath}")

        hdfs_connection = BaseHook.get_connection("yandex_cloud_hdfs")

        fs = pyhdfs.HdfsClient(hosts=f"{hdfs_connection.host}:{hdfs_connection.port}",
                               user_name=hdfs_connection.login)
        fs.mkdirs(HDFS_FOLDER)
        if fs.exists(hdfs_filepath):
            fs.delete(hdfs_filepath)
        fs.copy_from_local(local_filepath, hdfs_filepath)
        logging.info(f"Uploaded {local_filepath} to HDFS {hdfs_filepath}")
    # [END upload_hdfs_function]

    # [START cleanup_function]
    def cleanup(**kwargs):
        import logging

        ti = kwargs["ti"]
        local_filepath = ti.xcom_pull(task_ids="generate_synthetic_txs", key="local_filepath")

        logging.info(f"Removing file: {local_filepath}")
        if os.path.exists(local_filepath):
            os.remove(local_filepath)
            logging.info(f"File removed: {local_filepath}")
        else:
            logging.info(f"Can not delete the file as it doesn't exists: {local_filepath}")
    # [END cleanup_function]

    # [START main_flow]

    generate_synthetic_txs_task = PythonOperator(
        task_id="generate_synthetic_txs",
        python_callable=generate_synthetic_txs
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    upload_to_hdfs_task = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
    )

    generate_synthetic_txs_task >> [upload_to_s3_task, upload_to_hdfs_task] >> cleanup_task

# [END main_flow]
# [END synthetic_data_dag]
