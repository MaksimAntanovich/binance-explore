from datetime import datetime
from functools import reduce
from typing import List

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, BooleanType, TimestampType


class BtcUsdtTrades:

    schema = StructType([
        StructField("tradeId", LongType(), False),
        StructField("price", DoubleType(), False),
        StructField("qty", DoubleType(), False),
        StructField("quoteQty", DoubleType(), False),
        StructField("time", LongType(), False),
        StructField("isBuyerMaker", BooleanType(), False),
        StructField("isBestMatch", BooleanType(), False),
    ])

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(self, dates: List[datetime]):
        return reduce(lambda df1, df2: df1.unionByName(df2), [
            self.__read_date(_date) for _date in dates
        ]).checkpoint()

    def __csv_path(self, _date: datetime):
        return f"data/BTCUSDT-trades-{_date.strftime('%Y-%m-%d')}.zip"

    def __read_date(self, _date: datetime):
        zipped_file_path = self.__csv_path(_date)
        df = self.spark.createDataFrame(pd.read_csv(zipped_file_path, header=None, names=self.schema.names), schema=self.schema)
        return df.withColumn("date", lit(_date.date())).withColumn("timestamp", (col("time") / 1000).astype(TimestampType()))
