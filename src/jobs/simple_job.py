from pyspark.sql.functions import count, lit

from src.datatypes.btcusdt_trades import BtcUsdtTrades
from src.jobs.job import Job


class SimpleJob(Job):

    input = None
    output = None

    def extract(self):
        self.input = BtcUsdtTrades(self.spark).load(self.job_context.date_range).cache()

    def transform(self):
        if self.input is None:
            self.extract()
        self.output = self.input.groupBy("date").agg(count(lit(1)))
        return self.output

    def load(self):
        if self.output is None:
            self.transform()
        self.output.write.csv("output/row_count_by_date.csv")
