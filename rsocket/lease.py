import abc
from datetime import timedelta, datetime
from typing import Optional

from rsocket.exceptions import RSocketRejected
from rsocket.frame import LeaseFrame
from rsocket.helpers import to_milliseconds

MAX_31_BIT = pow(2, 31) - 1


class Lease(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def assert_request_allowed(self, stream_id: Optional[int] = None):
        ...

    @abc.abstractmethod
    def to_frame(self) -> LeaseFrame:
        ...


class NullLease(Lease):
    def assert_request_allowed(self, stream_id: Optional[int] = None):
        pass

    def to_frame(self) -> LeaseFrame:
        frame = LeaseFrame()
        frame.number_of_requests = MAX_31_BIT
        frame.time_to_live = MAX_31_BIT
        return frame


class DefinedLease(Lease):
    __slots__ = (
        '_maximum_request_count',
        '_request_counter',
        '_maximum_lease_time',
        '_lease_created_at'
    )

    def __init__(self,
                 maximum_request_count: int = MAX_31_BIT,
                 maximum_lease_time: timedelta = timedelta(milliseconds=MAX_31_BIT)):
        self._maximum_request_count = maximum_request_count
        self._maximum_lease_time = maximum_lease_time
        self._lease_created_at = datetime.now()
        self._request_counter = 0

    def assert_request_allowed(self, stream_id: Optional[int] = None):
        if not self._is_request_allowed():
            raise RSocketRejected(stream_id)

    def _is_request_allowed(self) -> bool:
        if self._lease_created_at + self._maximum_lease_time <= datetime.now():
            return False

        self._request_counter += 1

        if self._request_counter > self._maximum_request_count:
            return False

        return True

    def to_frame(self) -> LeaseFrame:
        frame = LeaseFrame()
        frame.number_of_requests = self._maximum_request_count
        frame.time_to_live = to_milliseconds(self._maximum_lease_time)
        return frame
