from typing import Dict, Type

from src.jobs.job import Job
from src.jobs.simple_job import SimpleJob

NAME_TO_JOB: Dict[str, Type[Job]] = {
    "SimpleJob": SimpleJob
}