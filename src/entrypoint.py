import argparse
import logging

from pyhocon import ConfigFactory

from src.jobcontext import JobContext
from src.jobs import NAME_TO_JOB


def run_job(args, config):
    with JobContext(config, args.job, args.start, args.end) as job_context:
        job = NAME_TO_JOB[args.job](job_context)
        job.extract()
        job.transform()
        job.load()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="PySpark jobs")
    parser.add_argument("-c", "--config", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    config = ConfigFactory.parse_file(args.config)

    run_job(args, config)
