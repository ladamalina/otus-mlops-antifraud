import findspark
findspark.init()
findspark.find()

import logging
import subprocess

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *


logging.basicConfig(level=logging.INFO)

app_name = "convert-csv-to-parquet-hdfs"
hdfs_host = "rc1a-dataproc-m-0phjwjdfohabk5n0.mdb.yandexcloud.net"
src_dir = "/fraud-data-src"
dest_dir = "/fraud-data-parquet"
s3_bucket = "mlops-data-nr"
s3_prefix = "/fraud-data-parquet"

spark = (
    SparkSession.builder
    .appName(app_name)
    .getOrCreate()
)
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

schema = StructType([StructField("transaction_id", LongType(), True),
                     StructField("tx_datetime", TimestampType(), True),
                     StructField("customer_id", LongType(), True),
                     StructField("terminal_id", LongType(), True),
                     StructField("tx_amount", DoubleType(), True),
                     StructField("tx_time_seconds", LongType(), True),
                     StructField("tx_time_days", LongType(), True),
                     StructField("tx_fraud", LongType(), True),
                     StructField("tx_fraud_scenario", LongType(), True)])


def convert_csv_to_parquet(spark_session, src_filepath, dest_filepath):
    logging.info(f"Converting {src_filepath} to {src_filepath}")

    src_filepath_hdfs = f"hdfs://{hdfs_host}{src_filepath}"
    logging.info(f"{src_filepath_hdfs=}")
    dest_filepath_hdfs = f"hdfs://{hdfs_host}{dest_filepath}"
    logging.info(f"{dest_filepath_hdfs=}")
    dest_file_s3 = f"s3a://{s3_bucket}{s3_prefix}/{Path(dest_filepath).name}"
    logging.info(f"{dest_file_s3=}")

    logging.info(f"Loading dataframe from {src_filepath_hdfs=}")
    df = spark_session.read.schema(schema).option("comment", "#").option("header", False).csv(src_filepath_hdfs)

    logging.info(f"Uploading dataframe to {dest_file_s3=}")
    df.write.parquet(dest_file_s3, mode="overwrite")

    logging.info(f"Writing dataframe to {dest_filepath_hdfs=}")
    df.write.parquet(dest_filepath_hdfs, mode="overwrite")

    logging.info(f"Success. Converted {src_filepath} to {src_filepath}")


cmd = f"hadoop fs -ls {src_dir} | sed '1d;s/  */ /g' | cut -d\  -f8"
files = subprocess.check_output(cmd, shell=True).strip().decode("utf-8").split('\n')

logging.info(f"Files to convert: {len(files)}")
for src_filepath in files:
    dest_filepath = f"{dest_dir}/{Path(src_filepath).stem}.parquet"
    convert_csv_to_parquet(spark, src_filepath, dest_filepath)

logging.info(f"All done")
