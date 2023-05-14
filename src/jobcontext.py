from pyhocon import ConfigTree
from pyspark.sql import SparkSession


class JobContext:
    def __init__(self, config: ConfigTree, job: str):
        self.config = config
        self.job = job
        self.spark: SparkSession = None

    def __enter__(self):
        self.spark = (
            SparkSession
            .builder
            .appName(self.job)
            .getOrCreate()
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()

