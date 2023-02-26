from __future__ import annotations

import json
import os
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

S3_BUCKET = os.getenv("S3_BUCKET", "mlops-data-nr")
S3_BUCKET_PREFIX_CSV = os.getenv("S3_BUCKET_PREFIX", "fraud-data/")
S3_BUCKET_PREFIX_PARQUET = os.getenv("S3_BUCKET_PREFIX", "fraud-data-parquet/")

with DAG(
    "convert_csv_to_parquet_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 0},
    # [END default_args]
    description="Convert CSV data to Parquet",
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2019, 8, 1, tz="UTC"),
    # catchup=False,
    tags=["mlops", "anti-fraud"],
    max_active_runs=1
) as dag:

    def download_csv_from_s3(**kwargs):
        import boto3
        import logging
        from airflow.hooks.base import BaseHook
        from botocore.exceptions import ClientError
        from pathlib import Path

        # filename = "2019-08-22"
        filename = kwargs['ds']

        filename_csv = filename + ".txt"
        local_dir = os.getenv("LOCAL_DIR", "/home/airflow/dags/convert-csv-to-parquet/")
        Path(local_dir).mkdir(parents=True, exist_ok=True)
        local_filepath_csv = Path(local_dir + filename_csv)
        object_name = S3_BUCKET_PREFIX_CSV + filename_csv

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
            logging.info(f"Downloading {filename_csv} from S3 bucket {S3_BUCKET} with key {object_name}")
            s3_client.download_file(S3_BUCKET, object_name, local_filepath_csv)
            logging.info(f"Saved to {local_filepath_csv}")
        except ClientError as e:
            logging.info(f"Error while downloading {filename_csv} from S3 bucket {S3_BUCKET} with key {object_name}")
            logging.error(e)
            raise

        ti = kwargs["ti"]
        ti.xcom_push(key="local_dir", value=local_dir)
        ti.xcom_push(key="filename_csv", value=filename_csv)
        ti.xcom_push(key="local_filepath_csv", value=str(local_filepath_csv))


    def convert_csv_to_parquet(**kwargs):
        import logging
        import numbers
        import numpy as np
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import warnings

        from pandas.errors import DtypeWarning
        from pathlib import Path
        from typing import Optional

        warnings.filterwarnings("ignore", category=DtypeWarning)

        ti = kwargs["ti"]
        local_dir = ti.xcom_pull(task_ids="download_csv_from_s3", key="local_dir")
        filename_csv = ti.xcom_pull(task_ids="download_csv_from_s3", key="filename_csv")
        local_filepath_csv = ti.xcom_pull(task_ids="download_csv_from_s3", key="local_filepath_csv")

        filename_parquet = filename_csv[:-3] + "parquet"
        local_filepath_parquet = Path(local_dir + filename_parquet)

        columns = ['tranaction_id', 'tx_datetime', 'customer_id', 'terminal_id', 'tx_amount', 'tx_time_seconds',
                   'tx_time_days', 'tx_fraud', 'tx_fraud_scenario']

        def get_chunksize_by_max_ram_mb(
                file_path: Path, max_ram_mb_per_chunk: int
        ) -> int:
            """Returns the amount of rows (chunksize) of a CSV that is approximately
            equivalent to the maximum RAM consumption defined.

            Args:
                file_path (Path): csv file path
                max_ram_mb_per_chunk (int): maximum consumption of RAM in mb

            Returns:
                int: chunksize
            """
            logging.info(f"get_chunksize_by_max_ram_mb")
            mb_size = file_path.stat().st_size / (1024 ** 2)
            num_lines = sum(1 for _ in open(file_path))
            rows_per_chunk = (
                    int(max_ram_mb_per_chunk / mb_size * num_lines / 3.5 / 10000) * 10000
            )
            logging.info(f"get_chunksize_by_max_ram_mb: {rows_per_chunk=}")

            return rows_per_chunk

        def auto_opt_pd_dtypes(
                df_: pd.DataFrame, inplace=False
        ) -> Optional[pd.DataFrame]:
            """Automatically downcast Number dtypes for minimal possible,
            will not touch other (datetime, str, object, etc)
            Ref.: https://stackoverflow.com/a/67403354
            :param df_: dataframe
            :param inplace: if False, will return a copy of input dataset
            :return: `None` if `inplace=True` or dataframe if `inplace=False`

            Opportunities for Improvement
            Optimize Object column for categorical
            Ref.: https://github.com/safurrier/data_science_toolbox/blob/master/data_science_toolbox/pandas/optimization/dtypes.py#L56
            """

            df = df_ if inplace else df_.copy()

            for col in df.columns:
                # integers
                if issubclass(df[col].dtypes.type, numbers.Integral):
                    # unsigned integers
                    if df[col].min() >= 0:
                        df[col] = pd.to_numeric(df[col], downcast="unsigned")
                    # signed integers
                    else:
                        df[col] = pd.to_numeric(df[col], downcast="integer")
                # other real numbers
                elif issubclass(df[col].dtypes.type, numbers.Real):
                    df[col] = pd.to_numeric(df[col], downcast="float")

            if not inplace:
                return df

        def get_dtype_opt(csv_file_path, sep, chunksize, encoding):
            """
            Identifies the optimized data type of each column by analyzing
            the entire dataframe by chunks.
            Ref.: https://stackoverflow.com/a/15556579

            return: dtype dict to pass as dtype argument of pd.read_csv
            """

            # list_chunk = pd.read_csv(
            #     csv_file_path,
            #     sep=sep,
            #     chunksize=chunksize,
            #     header=0,
            #     low_memory=True,
            #     encoding=encoding,
            # )
            logging.info(f"get_dtype_opt")
            list_chunk = pd.read_csv(
                csv_file_path,
                sep=sep,
                chunksize=chunksize,
                skiprows=1,
                header=None,
                names=columns,
                low_memory=True,
                encoding=encoding,
            )
            list_chunk_opt = []
            for chunk in list_chunk:
                chunk_opt = auto_opt_pd_dtypes(chunk, inplace=False)
                list_chunk_opt.append(chunk_opt.dtypes)
            df_dtypes = pd.DataFrame(list_chunk_opt)
            dict_dtypes = df_dtypes.apply(
                lambda x: np.result_type(*x), axis=0
            ).to_dict()

            return dict_dtypes

        def get_chunksize_opt(
                csv_file_path, sep, dtype, max_ram_mb_per_chunk, chunksize, encoding
        ):
            """After dtype optimization, analyzing only one data chunk,
            returns the amount of rows (chunksize) of a CSV that is
            approximately equivalent to the maximum RAM consumption.
            """
            logging.info(f"get_chunksize_opt")
            for chunk in pd.read_csv(
                    csv_file_path,
                    sep=sep,
                    dtype=dtype,
                    skiprows=1,
                    header=None,
                    names=columns,
                    chunksize=chunksize,
                    low_memory=True,
                    encoding=encoding,
            ):
                chunksize_opt = chunksize * (
                        max_ram_mb_per_chunk
                        / (chunk.memory_usage(deep=True).sum() / (1024 ** 2))
                )
                break
            return int(chunksize_opt / 10_000) * 10_000

        def write_parquet(
                csv_file_path, parquet_file_path, sep, dtype, chunksize, encoding
        ):
            """Write Parquet file from a CSV with defined dtypes and
            by chunks for RAM optimization.
            """
            logging.info(f"write_parquet")

            for i, chunk in enumerate(
                    pd.read_csv(
                        csv_file_path,
                        sep=sep,
                        dtype=dtype,
                        skiprows=1,
                        header=None,
                        names=columns,
                        chunksize=chunksize,
                        low_memory=True,
                        encoding=encoding,
                    )
            ):
                logging.info(f"Converting chunk {i} ({chunksize=})")
                if i == 0:
                    # Guess the schema of the CSV file from the first chunk
                    parquet_schema = pa.Table.from_pandas(df=chunk).schema
                    # Open a Parquet file for writing
                    parquet_writer = pq.ParquetWriter(
                        parquet_file_path, parquet_schema, compression="gzip"
                    )
                # Write CSV chunk to the parquet file
                table = pa.Table.from_pandas(chunk, schema=parquet_schema)
                parquet_writer.write_table(table)

            parquet_writer.close()

        def convert_csv_to_parquet(
                csv_file_path,
                parquet_file_path,
                max_ram_mb_per_chunk,
                sep=",",
                encoding="utf8",
        ):
            """Converts a CSV file to Parquet file, with maximum RAM consumption
            limit allowed and automatically optimizing the data types of each column.
            """
            chunksize = get_chunksize_by_max_ram_mb(
                csv_file_path, max_ram_mb_per_chunk
            )
            dict_dtypes_opt = get_dtype_opt(csv_file_path, sep, chunksize, encoding)
            chunksize_opt = get_chunksize_opt(
                csv_file_path,
                sep,
                dict_dtypes_opt,
                max_ram_mb_per_chunk,
                chunksize,
                encoding,
            )
            write_parquet(
                csv_file_path,
                parquet_file_path,
                sep,
                dict_dtypes_opt,
                chunksize_opt,
                encoding,
            )

        logging.info(f"Converting {filename_csv} to {filename_parquet}")
        # read_options = csv.ReadOptions(skip_rows=1, column_names=columns)
        # table = csv.read_csv(local_filepath_csv, read_options=read_options)
        # parquet.write_table(table, local_filepath_parquet)
        convert_csv_to_parquet(
            csv_file_path=Path(local_filepath_csv),
            parquet_file_path=Path(local_filepath_parquet),
            max_ram_mb_per_chunk=256, sep=",", encoding="utf8")

        logging.info(f"Converted file saved to {local_filepath_parquet}")

        ti.xcom_push(key="filename_parquet", value=filename_parquet)
        ti.xcom_push(key="local_filepath_parquet", value=str(local_filepath_parquet))


    def upload_to_s3(**kwargs):
        import boto3
        import logging
        from airflow.hooks.base import BaseHook
        from botocore.exceptions import ClientError

        ti = kwargs["ti"]
        filename_parquet = ti.xcom_pull(task_ids="convert_csv_to_parquet", key="filename_parquet")
        local_filepath_parquet = ti.xcom_pull(task_ids="convert_csv_to_parquet", key="local_filepath_parquet")
        object_name = S3_BUCKET_PREFIX_PARQUET + filename_parquet

        logging.info(f"Uploading {local_filepath_parquet} to S3 bucket {S3_BUCKET} with key {object_name}")

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
            s3_client.upload_file(local_filepath_parquet, S3_BUCKET, object_name)
            logging.info(f"Uploaded {local_filepath_parquet} to S3 bucket {S3_BUCKET} with key {object_name}")
        except ClientError as e:
            logging.info(f"Error while uploading {local_filepath_parquet} to S3 bucket {S3_BUCKET} with key {object_name}")
            logging.error(e)
            raise


    def cleanup(**kwargs):
        import logging

        ti = kwargs["ti"]
        local_filepath_csv = ti.xcom_pull(task_ids="download_csv_from_s3", key="local_filepath_csv")
        local_filepath_parquet = ti.xcom_pull(task_ids="convert_csv_to_parquet", key="local_filepath_parquet")

        logging.info(f"Removing file: {local_filepath_csv=}")
        if local_filepath_csv and os.path.exists(local_filepath_csv):
            os.remove(local_filepath_csv)
            logging.info(f"File removed: {local_filepath_csv}")
        else:
            logging.info(f"Can not delete the file as it doesn't exists: {local_filepath_csv=}")

        logging.info(f"Removing file: {local_filepath_parquet=}")
        if local_filepath_parquet and os.path.exists(local_filepath_parquet):
            os.remove(local_filepath_parquet)
            logging.info(f"File removed: {local_filepath_parquet}")
        else:
            logging.info(f"Can not delete the file as it doesn't exists: {local_filepath_parquet=}")


    download_csv_from_s3_task = PythonOperator(
        task_id="download_csv_from_s3",
        python_callable=download_csv_from_s3
    )

    convert_csv_to_parquet_task = PythonOperator(
        task_id="convert_csv_to_parquet",
        python_callable=convert_csv_to_parquet
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        trigger_rule="all_done"
    )

    download_csv_from_s3_task >> convert_csv_to_parquet_task >> upload_to_s3_task >> cleanup_task
