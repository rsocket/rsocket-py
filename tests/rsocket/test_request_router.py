import pytest

from rsocket.exceptions import RSocketEmptyRoute
from rsocket.helpers import create_response
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter


async def test_request_router_exception_on_duplicate_route_with_same_type():
    router = RequestRouter()

    with pytest.raises(KeyError):
        @router.response('path1')
        async def request_response(payload, composite_metadata) -> Awaitable[Payload]:
            return create_response()

        @router.response('path1')
        async def request_response2(payload, composite_metadata) -> Awaitable[Payload]:
            return create_response()


async def test_request_router_disallow_empty_routes():
    router = RequestRouter()

    with pytest.raises(RSocketEmptyRoute):
        @router.response('')
        async def request_response(payload, composite_metadata) -> Awaitable[Payload]:
            return create_response()

    with pytest.raises(RSocketEmptyRoute):
        # noinspection PyTypeChecker
        @router.response(None)
        async def request_response2(payload, composite_metadata) -> Awaitable[Payload]:
            return create_response()
