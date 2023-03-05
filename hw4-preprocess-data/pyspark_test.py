import findspark
findspark.init()

from pyspark.sql import SparkSession

app_name = "pyspark_test"

spark = (SparkSession.builder
    .master("yarn")
    .appName(app_name)
    .getOrCreate()
)
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

filepath_auto = "hdfs://rc1a-dataproc-m-0phjwjdfohabk5n0.mdb.yandexcloud.net/fraud-data-auto/2023-02-26.parquet"
spark.read.parquet(filepath_auto).printSchema()

spark.stop()
