from pyspark.sql import SparkSession


def get_spark_session(env):
    builder = SparkSession.builder \
        .config('spark.driver.extraJavaOptions', '-Dlog4j.configuration=file:log4j.properties') \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.execution.arrow.enabled", "false") \
        .enableHiveSupport()

    if env == "LOCAL":
        builder = builder.master("local[2]")

    return builder.getOrCreate()



