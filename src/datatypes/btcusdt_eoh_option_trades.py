from datetime import datetime
from functools import reduce
from typing import List

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DateType, StructField, ByteType, StringType, IntegerType, DoubleType


class BtcUsdtEohOptionTrades:

    schema = StructType([
        StructField("date", DateType(), False),
        StructField("hour", ByteType(), False),
        StructField("symbol", StringType(), False),
        StructField("underlying", StringType(), False),
        StructField("type", StringType(), False),
        StructField("strike", StringType(), False),
        StructField("open", IntegerType(), False),
        StructField("high", IntegerType(), False),
        StructField("low", IntegerType(), False),
        StructField("close", IntegerType(), False),
        StructField("volume_contracts", DoubleType(), False),
        StructField("volume_usdt", DoubleType(), False),
        StructField("best_bid_price", IntegerType(), False),
        StructField("best_ask_price", IntegerType(), False),
        StructField("best_bid_qty", DoubleType(), False),
        StructField("best_ask_qty", DoubleType(), False),
        StructField("best_buy_iv", DoubleType(), True),
        StructField("best_sell_iv", DoubleType(), True),
        StructField("mark_price", IntegerType(), False),
        StructField("mark_iv", DoubleType(), False),
        StructField("delta", DoubleType(), False),
        StructField("gamma", DoubleType(), False),
        StructField("vega", DoubleType(), False),
        StructField("theta", DoubleType(), False),
        StructField("openinterest_contracts", DoubleType(), False),
        StructField("openinterest_usdt", DoubleType(), False),
    ])

    spark_to_dtype = {
        "integer": np.int32,
        "double": np.float64,
        "byte": np.int8
    }

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.pandas_schema = {field.name: self.spark_to_dtype[field.dataType.typeName()] for field in self.schema if
                         field.dataType.typeName() in self.spark_to_dtype}

    def load(self, dates: List[datetime]):
        return reduce(lambda df1, df2: df1.unionByName(df2), [
            self.__read_date(_date) for _date in dates
        ]).checkpoint()

    def __csv_path(self, _date: datetime):
        return f"data/option/BTCUSDT-EOHSummary-{_date.strftime('%Y-%m-%d')}.zip"

    def __read_date(self, _date: datetime):
        zipped_file_path = self.__csv_path(_date)
        df = self.spark.createDataFrame(pd.read_csv(zipped_file_path, header=0, parse_dates=["date"], dtype=self.pandas_schema), schema=self.schema)
        return df