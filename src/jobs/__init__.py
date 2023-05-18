from typing import Dict, Type

from src.jobs.job import Job
from src.jobs.simple_job import SimpleJob
from src.jobs.stats_per_minute_job import StatsPerMinuteJob

NAME_TO_JOB: Dict[str, Type[Job]] = {
    "SimpleJob": SimpleJob,
    "StatsPerMinuteJob": StatsPerMinuteJob,
}