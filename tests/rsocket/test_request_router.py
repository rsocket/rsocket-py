from asyncio import Future

import pytest

from rsocket.helpers import create_future
from rsocket.routing.request_router import RequestRouter


async def test_request_router_exception_on_duplicate_route_with_same_type():
    router = RequestRouter()

    with pytest.raises(KeyError):
        @router.response('path1')
        async def request_response(payload, composite_metadata) -> Future:
            return create_future()

        @router.response('path1')
        async def request_response2(payload, composite_metadata) -> Future:
            return create_future()
