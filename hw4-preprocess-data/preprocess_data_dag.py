from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    "preprocess_data_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 0},
    # [END default_args]
    description="Preprocess data",
    schedule_interval="@once",
    start_date=pendulum.datetime(2022, 11, 27, tz="UTC"),
    catchup=False,
    tags=["mlops", "anti-fraud"],
    max_active_runs=1
) as dag:

    generate_transaction_data_task = SparkSubmitOperator(
        task_id="preprocess_data",
        name="preprocess_data_dag",
        conn_id="yandex_cloud_spark",
        application="/home/airflow/dags/preprocess_data.py",
        application_args=[
            "--date_from", "{{ ds }}",
            "--date_to", "{{ ds }}",
            # "--date_from", "2023-02-20",
            # "--date_to", "2023-02-28",
            "--hdfs_host", "{{ conn.yandex_cloud_hdfs.host }}",
            # "--hdfs_dirs_input", "/fraud-data-parquet/*.parquet,/fraud-data-auto/*.parquet",
            "--hdfs_dirs_input", "/fraud-data-auto/{{ ds }}.parquet",
            "--hdfs_dir_output", "/fraud-data-processed",
            "--s3_bucket", "mlops-data-nr",
            "--s3_bucket_prefix", "/fraud-data-processed",
        ],
        env_vars={"HADOOP_CONF_DIR": "/etc/hadoop/conf"},
        executor_cores=2,
        # executor_memory="4g",
        # driver_memory="4g",
    )
