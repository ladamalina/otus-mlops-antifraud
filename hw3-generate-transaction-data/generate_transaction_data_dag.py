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
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
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
        jars="/usr/lib/spark/jars/hadoop-aws-3.2.2.jar,/usr/lib/spark/jars/aws-java-sdk-bundle-1.11.563.jar,/usr/lib/spark/jars/iam-s3-credentials.jar",
        env_vars={"HADOOP_CONF_DIR": "/etc/hadoop/conf"},
        # executor_cores=2,
        # executor_memory="2g",
        # driver_memory="2g",
    )
