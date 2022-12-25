from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    "generate_transaction_data_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 0},
    # [END default_args]
    description="Generate transaction data",
    schedule_interval="@once",
    start_date=pendulum.datetime(2022, 12, 4, tz="UTC"),
    catchup=False,
    tags=["mlops", "anti-fraud"],
    max_active_runs=1
) as dag:

    generate_transaction_data_task = SparkSubmitOperator(
        task_id="generate_transaction_data",
        name="generate_transaction_data_dag",
        conn_id="yandex_cloud_spark",
        application="/home/airflow/dags/generate_transaction_data.py",
        application_args=[
            "--i_customers", str(500000),
            "--i_terminals", str(1000),
            "--i_days", str(1),
            "--i_start_date", "{{ ds }}",
            "--hdfs_host", "{{ conn.yandex_cloud_hdfs.host }}",
            "--hdfs_dir_output", "/fraud-data-auto",
            "--s3_bucket", "mlops-data-nr",
            "--s3_bucket_prefix", "/fraud-data-auto",
        ],
        env_vars={"HADOOP_CONF_DIR": "/etc/hadoop/conf"},
        conf={"spark.sql.broadcastTimeout": str(60*60*3)},
        executor_cores=2,
        executor_memory="4g",
        driver_memory="4g",
    )
