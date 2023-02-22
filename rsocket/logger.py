import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta


def logger():
    return logging.getLogger('pyrsocket')


@dataclass
class Result:
    time: timedelta = None


@contextmanager
def measure_runtime():
    result = Result()
    start = datetime.now()
    yield result
    result.time = datetime.now() - start
