from src.jobs.job import Job


class SimpleJob(Job):
    def extract(self):
        self.data = self.spark.read.csv()

    def transform(self):
        super().transform()

    def load(self):
        super().load()