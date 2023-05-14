import logging
from datetime import date

from src.datatypes.btcusdt_trades import BtcUsdtTrades
from src.jobs.job import Job


class SimpleJob(Job):
    def extract(self):
        self.data = BtcUsdtTrades(self.spark).load([date(2023, 4, 1)])

    def transform(self):
        self.rowcount = self.data.count()

    def load(self):
        logging.info(f"{self.rowcount} BTC/USDT trades")