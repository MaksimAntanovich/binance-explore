from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    minute,
    median,
    max,
    min,
    mean,
    stddev,
    sum,
    expr,
    to_timestamp
)
from src.datatypes.btcusdt_trades import BtcUsdtTrades
from src.jobs import Job


class StatsPerMinuteJob(Job):

    def extract(self):
        self.input = BtcUsdtTrades(self.spark).load(self.job_context.date_range).cache()

    def transform(self):
        if self.input is None:
            self.extract()
        self.output = (self.input
         .withColumn("date", to_date(col("timestamp")))
         .withColumn("hour", hour(col("timestamp")))
         .withColumn("minute", minute(col("timestamp")))
         .groupBy(["date", "hour", "minute"])
         .agg(
            median(col("price")).alias("price_median"),
            max(col("price")).alias("price_max"),
            min(col("price")).alias("price_min"),
            mean(col("price")).alias("price_mean"),
            stddev(col("price")).alias("price_std"),
            sum(col("qty")).alias("qty_sum"))
         .select(
            (
                to_timestamp(col("date")) +
                expr("make_interval(0,0,0,0,hour,0,0)") +
                expr("make_interval(0,0,0,0,0,minute,0)")
            ).alias("timestamp_minutes"),
            "price_median",
            "price_max",
            "price_min",
            "price_mean",
            "price_std",
            "qty_sum",
          )
         )

    def load(self):
        if self.output is None:
            self.transform()
        self.output.write.csv("output/stats_per_minute.csv")