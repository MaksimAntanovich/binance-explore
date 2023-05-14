import pandas as pd
from pyhocon import ConfigTree
from pyspark.sql import SparkSession


class JobContext:
    def __init__(self, config: ConfigTree, job: str, start: str, end: str):
        self.config = config
        self.job = job
        self.date_range = pd.date_range(start, end, freq='D')
        self.spark: SparkSession = None

    def create_spark_session(self):
        spark = (
            SparkSession
            .builder
            .appName(self.job)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(self.config["job"]["loglevel"])
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        return spark


    def __enter__(self):
        self.spark = self.create_spark_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()

