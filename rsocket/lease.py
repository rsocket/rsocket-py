import abc
import time
from datetime import timedelta, datetime
from typing import Optional

from reactivestreams.publisher import AsyncPublisher
from reactivestreams.subscriber import Subscriber
from rsocket.frame import LeaseFrame
from rsocket.helpers import to_milliseconds

MAX_31_BIT = pow(2, 31) - 1


class Lease(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def is_request_allowed(self, stream_id: Optional[int] = None) -> bool:
        ...

    @abc.abstractmethod
    def to_frame(self) -> LeaseFrame:
        ...


class NullLease(Lease):
    def is_request_allowed(self, stream_id: Optional[int] = None) -> bool:
        return True

    def to_frame(self) -> LeaseFrame:
        frame = LeaseFrame()
        frame.number_of_requests = MAX_31_BIT
        frame.time_to_live = MAX_31_BIT
        return frame


class DefinedLease(Lease):
    __slots__ = (
        'maximum_request_count',
        '_request_counter',
        'maximum_lease_time',
        '_lease_created_at'
    )

    def __str__(self) -> str:
        return '{maximum_request_count: %s, lease_ttl: %s}' % (self.maximum_request_count, self.maximum_lease_time)

    def __init__(self,
                 maximum_request_count: int = MAX_31_BIT,
                 maximum_lease_time: timedelta = timedelta(milliseconds=MAX_31_BIT)):
        self.maximum_request_count = maximum_request_count
        self.maximum_lease_time = maximum_lease_time
        self._lease_created_at = datetime.now()
        self._request_counter = 0

    def is_request_allowed(self, stream_id: Optional[int] = None):
        return self._is_request_allowed()

    def _is_request_allowed(self) -> bool:
        if self._lease_created_at + self.maximum_lease_time <= datetime.now():
            return False

        self._request_counter += 1

        if self._request_counter > self.maximum_request_count:
            return False

        return True

    def to_frame(self) -> LeaseFrame:
        frame = LeaseFrame()
        frame.number_of_requests = self.maximum_request_count
        frame.time_to_live = to_milliseconds(self.maximum_lease_time)
        return frame


class LeasePublisher(AsyncPublisher):
    async def subscribe(self, subscriber: Subscriber):
        pass


class SingleLeasePublisher(LeasePublisher):
    def __init__(self,
                 maximum_request_count: int = MAX_31_BIT,
                 maximum_lease_time: timedelta = timedelta(milliseconds=MAX_31_BIT),
                 sleep_time=timedelta(seconds=0)
                 ):
        self.sleep_time = sleep_time
        self.maximum_lease_time = maximum_lease_time
        self.maximum_request_count = maximum_request_count

    async def subscribe(self, subscriber: Subscriber):
        time.sleep(self.sleep_time.total_seconds())

        await subscriber.on_next(DefinedLease(
            maximum_request_count=self.maximum_request_count,
            maximum_lease_time=self.maximum_lease_time
        ))
