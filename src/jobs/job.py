from src.jobcontext import JobContext


class Job:
    input = None
    output = None

    def __init__(self, job_context: JobContext):
        self.job_context = job_context
        self.spark = job_context.spark

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass