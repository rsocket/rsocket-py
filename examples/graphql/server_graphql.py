import json
import logging
import sys
from typing import AsyncGenerator, Tuple

from graphql import execute, parse, subscribe
from graphql_server import get_graphql_params
from quart import Quart

from examples.graphql.schema import AsyncSchema
from rsocket.frame_helpers import str_to_bytes
from rsocket.graphql.helpers import execute_query_in_payload
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.transports.quart_websocket import websocket_handler

app = Quart(__name__)

router = RequestRouter()


@router.response('graphql')
async def graphql_query(payload: Payload):
    execution_result = await execute_query_in_payload(payload)

    response_data = str_to_bytes(json.dumps({
        'data': execution_result.data
    }))

    return create_future(Payload(response_data))


@router.stream('graphql')
async def graphql_subscription(payload: Payload):
    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        data = json.loads(payload.data.decode('utf-8'))
        params = get_graphql_params(data, {})
        schema = AsyncSchema
        document = parse(params.query)

        async for execution_result in await subscribe(
                schema,
                document,
                variable_values=params.variables,
                operation_name=params.operation_name
        ):
            response_data = str_to_bytes(json.dumps({
                'data': execution_result.data
            }))
            yield Payload(response_data)

    return StreamFromAsyncGenerator(generator)


@router.response('ping')
async def ping():
    return create_future(Payload(b'pong'))


def handler_factory():
    return RoutingRequestHandler(router)


@app.websocket("/")
async def ws():
    await websocket_handler(handler_factory=handler_factory)


if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else 7000
    logging.basicConfig(level=logging.DEBUG)
    app.run(port=port)
