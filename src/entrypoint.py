import argparse
import logging

from pyhocon import ConfigFactory

from src.jobcontext import JobContext
from src.jobs import NAME_TO_JOB

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="PySpark jobs")
    parser.add_argument("-c", "--config", required=True)
    parser.add_argument("--job", required=True)
    args = parser.parse_args()

    config = ConfigFactory.parse_file(args.config)

    logging.root.setLevel(config["job"]["loglevel"])

    with JobContext(config, args.job) as job_context:
        job = NAME_TO_JOB[args.job](job_context)
        job.extract()
        job.transform()
        job.load()