import argparse
import datetime as dt
import findspark
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import dayofweek, col, when, hour
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import List


schema = StructType([StructField("transaction_id", LongType(), True),
                     StructField("tx_datetime", TimestampType(), True),
                     StructField("customer_id", LongType(), True),
                     StructField("terminal_id", LongType(), True),
                     StructField("tx_amount", DoubleType(), True),
                     StructField("tx_time_seconds", LongType(), True),
                     StructField("tx_time_days", LongType(), True),
                     StructField("tx_fraud", LongType(), True),
                     StructField("tx_fraud_scenario", LongType(), True)])
dt_format = "%Y-%m-%d"
dt_format_full = f"{dt_format} %H:%M:%S"
dt_today = dt.datetime.today()
dt_today_str = dt_today.strftime(dt_format)
windows_size_in_days = [1, 7, 30]
extra_window_in_days = max(windows_size_in_days)
days_to_seconds = lambda i: i * 86400
delay_window = 7


def get_customer_spending_behaviour_features(spark_df):
    for window_size in windows_size_in_days:
        windowSpec = Window().partitionBy(["customer_id"])\
            .orderBy(col("tx_datetime").cast("timestamp").cast("long"))\
            .rangeBetween(-days_to_seconds(window_size), 0)
        spark_df = spark_df.withColumn("customer_id_nb_tx_" + str(window_size) + "day_window",
                                       F.count("*").over(windowSpec)).orderBy("tx_datetime")
        spark_df = spark_df.withColumn("customer_id_avg_amount_" + str(window_size) + "day_window",
                                       F.mean("tx_amount").over(windowSpec)).orderBy("tx_datetime")

    return spark_df


def get_count_risk_rolling_window(spark_df):
    delay_period = days_to_seconds(delay_window)

    windowSpec = Window().partitionBy(["terminal_id"])\
        .orderBy(col("tx_datetime").cast("timestamp").cast("long"))\
        .rangeBetween(-delay_period, 0)
    spark_df = spark_df.withColumn("nb_fraud_delay", F.sum("tx_fraud").over(windowSpec)).orderBy("tx_datetime")
    spark_df = spark_df.withColumn("nb_tx_delay", F.count("tx_fraud").over(windowSpec)).orderBy("tx_datetime")

    for window_size in windows_size_in_days:
        windowSpec = Window().partitionBy(["terminal_id"])\
            .orderBy(col("tx_datetime").cast("timestamp").cast("long"))\
            .rangeBetween(-days_to_seconds(window_size) - delay_period, 0)
        spark_df = spark_df.withColumn("nb_fraud_delay_window",
                                       F.sum("tx_fraud").over(windowSpec)).orderBy("tx_datetime")
        spark_df = spark_df.withColumn("nb_tx_delay_window",
                                       F.count("tx_fraud").over(windowSpec)).orderBy("tx_datetime")
        spark_df = spark_df.withColumn("nb_fraud_window",
                                       spark_df["nb_fraud_delay_window"] - spark_df["nb_fraud_delay"])

        spark_df = spark_df.withColumn("terminal_id_nb_tx_" + str(window_size) + "day_window",
                                       spark_df["nb_tx_delay_window"] - spark_df["nb_tx_delay"])
        spark_df = spark_df.withColumn("terminal_id_risk_" + str(window_size) + "day_window",
                                       spark_df["nb_fraud_window"] /
                                       spark_df["terminal_id_nb_tx_" + str(window_size) + "day_window"])

    spark_df = spark_df.na.fill(0)
    spark_df = spark_df.drop("nb_fraud_delay", "nb_tx_delay", "nb_fraud_delay_window", "nb_tx_delay_window",
                             "nb_fraud_window")

    return spark_df


def preprocess_transactions(spark_df):
    spark_df = spark_df.withColumn("tx_during_weekend", dayofweek(spark_df.tx_datetime))
    spark_df = spark_df.withColumn("tx_during_weekend",
                                   when((spark_df["tx_during_weekend"] == 1) | (spark_df["tx_during_weekend"] == 7), 1)
                                   .otherwise(0))

    spark_df = spark_df.withColumn("tx_during_night", hour(spark_df.tx_datetime))
    spark_df = spark_df.withColumn("tx_during_night", when(spark_df["tx_during_night"] <= 6, 1).otherwise(0))

    spark_df = get_customer_spending_behaviour_features(spark_df)
    spark_df = get_count_risk_rolling_window(spark_df)

    return spark_df


def get_preprocessed_data(spark: SparkSession, dt_from: dt.datetime, dt_to: dt.datetime, hdfs_dirs_input: List[str]):
    # hdfs_dirs_input = [f"{_}/*.parquet" for _ in hdfs_dirs_input]
    logging.info(f"Reading {hdfs_dirs_input}")
    df = spark.read.schema(schema).parquet(*hdfs_dirs_input)
    dt_from_extra = dt_from - dt.timedelta(days=extra_window_in_days)
    logging.info(f"Extending {dt_from=} using {extra_window_in_days=}, {dt_from_extra=}")
    logging.info(f"Preprocess data using window {dt_from_extra} <= dt <= {dt_to}")
    transactions_df = df.filter((df.tx_datetime >= dt_from_extra) & (df.tx_datetime <= dt_to))
    logging.info(f"Rows to process: {transactions_df.count()}")
    transactions_df = preprocess_transactions(transactions_df)

    return transactions_df


def save_processed_data_to_hdfs(spark_df, hdfs_host_output: str, hdfs_dir_output: str, filename_output: str):
    filepath_hdfs = f"hdfs://{hdfs_host_output}{hdfs_dir_output}/{filename_output}"
    logging.info(f"Writing dataframe to {filepath_hdfs=}")
    spark_df.write.parquet(filepath_hdfs, mode="overwrite")


def save_processed_data_to_s3(spark_df, s3_bucket: str, s3_bucket_prefix: str, filename_output: str):
    filepath_s3 = f"s3a://{s3_bucket}{s3_bucket_prefix}/{filename_output}"
    logging.info(f"Uploading data to {filepath_s3=}")
    spark_df.write.parquet(filepath_s3, mode="overwrite")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    findspark.init()
    findspark.find()

    spark = (
        SparkSession.builder
        .appName("preprocess_data")
        .master("yarn")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--date_from", type=str, default=dt_today_str)
    parser.add_argument("--date_to", type=str, default=dt_today_str)
    parser.add_argument("--hdfs_host", type=str, required=True)
    parser.add_argument("--hdfs_dirs_input", type=str, required=True,
                        help="HDFS folders with source data, comma separated")
    parser.add_argument("--hdfs_dir_output", type=str, required=True)
    parser.add_argument("--s3_bucket", type=str, required=True)
    parser.add_argument("--s3_bucket_prefix", type=str, required=True)
    args = parser.parse_args()

    dt_from = dt.datetime.strptime(f"{args.date_from} 00:00:00", dt_format_full)
    dt_to = dt.datetime.strptime(f"{args.date_to} 23:59:59", dt_format_full)
    hdfs_dirs_input = args.hdfs_dirs_input.split(",")

    logging.info(f"{args.date_from=}")
    logging.info(f"{dt_from=}")
    logging.info(f"{args.date_to=}")
    logging.info(f"{dt_to=}")
    logging.info(f"{args.hdfs_host=}")
    logging.info(f"{args.hdfs_dirs_input=}")
    logging.info(f"{hdfs_dirs_input=}")
    logging.info(f"{args.hdfs_dir_output=}")
    logging.info(f"{args.s3_bucket=}")
    logging.info(f"{args.s3_bucket_prefix=}")

    df = get_preprocessed_data(spark, dt_from, dt_to, hdfs_dirs_input)

    days_n = (dt_to - dt_from).days + 1
    for day_num in range(days_n):
        dt_save = dt_from + dt.timedelta(days=day_num)
        dt_save_from = dt_save.replace(minute=0, hour=0, second=0)
        dt_save_to = dt_save.replace(minute=59, hour=23, second=59)
        day_df = df.filter((df.tx_datetime >= dt_save_from) & (df.tx_datetime <= dt_save_to))
        logging.info(f"Saving day {day_num+1}/{days_n}, {day_df.count()} rows")
        if day_df.count() > 0:
            filename_output = f"{dt_save.strftime(dt_format)}.parquet"
            save_processed_data_to_hdfs(day_df, args.hdfs_host, args.hdfs_dir_output, filename_output)
            save_processed_data_to_s3(day_df, args.s3_bucket, args.s3_bucket_prefix, filename_output)

    spark.stop()


if __name__ == "__main__":
    main()
