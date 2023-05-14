from datetime import date
from functools import reduce
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType, BooleanType


class BtcUsdtTrades:

    schema = StructType([
        StructField("tradeId", LongType(), False),
        StructField("price", DoubleType(), False),
        StructField("qty", DoubleType(), False),
        StructField("quoteQty", DoubleType(), False),
        StructField("time", TimestampType(), False),
        StructField("isBuyerMaker", BooleanType(), False),
        StructField("isBestMatch", BooleanType(), False),
    ])

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(self, dates: List[date]):
        return reduce(lambda df1, df2: df1.unionByName(df2), [
            self.spark.read.csv(self.__csv_path(_date), schema=self.schema, header=False) for _date in dates
        ]).checkpoint()

    def __csv_path(self, _date: date):
        return f"data/BTCUSDT-trades-{_date.strftime('%Y-%m-%d')}.zip"
