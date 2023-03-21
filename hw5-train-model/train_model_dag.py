from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    "train_model_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 0},
    # [END default_args]
    description="Train model",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 3, 14, tz="UTC"),
    catchup=False,
    tags=["mlops", "anti-fraud"],
    max_active_runs=1
) as dag:

    generate_transaction_data_task = SparkSubmitOperator(
        task_id="train_model",
        name="train_model_dag",
        conn_id="yandex_cloud_spark",
        application="/home/airflow/dags/train_model.py",
        application_args=[
            "--date_from", "{{ ds }}",
            "--date_to", "{{ ds }}",
            # "--date_from", "2023-03-14",
            # "--date_to", "2023-03-20",
            "--hdfs_host", "{{ conn.yandex_cloud_hdfs.host }}",
            # "--hdfs_dirs_input", "/fraud-data-processed/*.parquet",
            "--hdfs_dirs_input", "/fraud-data-processed/{{ ds }}.parquet",
        ],
        env_vars={"HADOOP_CONF_DIR": "/etc/hadoop/conf"},
        conf={
            "spark.yarn.appMasterEnv.MLFLOW_S3_ENDPOINT_URL": "{{ conn.yandex_cloud_s3.extra_dejson.endpoint_url }}",
            "spark.yarn.appMasterEnv.MLFLOW_TRACKING_URI": "{{ conn.mlflow.host }}",
            "spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID": "{{ conn.yandex_cloud_s3.login }}",
            "spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY": "{{ conn.yandex_cloud_s3.password }}",
            "spark.yarn.appMasterEnv.AWS_DEFAULT_REGION": "{{ conn.yandex_cloud_s3.extra_dejson.region_name }}",
        },
        executor_cores=2,
        # executor_memory="4g",
        # driver_memory="4g",
    )
