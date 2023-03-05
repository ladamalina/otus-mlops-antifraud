from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    "pyspark_test_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 0},
    # [END default_args]
    description="Test simple PySpark script",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=False,
    tags=["mlops", "anti-fraud"],
    max_active_runs=1
) as dag:

    spark_submit_local = SparkSubmitOperator(
        task_id='spark_submit_task_to_hdfs',
        conn_id='yandex_cloud_spark',
        application='/home/airflow/dags/pyspark-test.py',
        env_vars={"HADOOP_CONF_DIR": "/etc/hadoop/conf"},
        # executor_cores=1,
        # executor_memory='2g',
        # driver_memory='2g',
        # conf={"spark.dynamicAllocation.enabled": "false",
        #       "spark.shuffle.service.enabled": "false"},
    )
