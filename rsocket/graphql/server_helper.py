import json
from typing import AsyncGenerator, Tuple

from graphql import subscribe, parse, GraphQLSchema

from rsocket.frame_helpers import str_to_bytes
from rsocket.graphql.helpers import execute_query_in_payload, get_graphql_params
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator


def graphql_handler(schema: GraphQLSchema, route: str):
    router = RequestRouter()

    @router.response(route)
    async def graphql_query(payload: Payload):
        execution_result = await execute_query_in_payload(payload, schema)

        response_data = str_to_bytes(json.dumps({
            'data': execution_result.data
        }))

        return create_future(Payload(response_data))

    @router.stream(route)
    async def graphql_subscription(payload: Payload):
        async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
            data = json.loads(payload.data.decode('utf-8'))
            params = get_graphql_params(data, {})
            document = parse(params.query)

            async for execution_result in await subscribe(
                    schema,
                    document,
                    operation_name=params.operation_name
            ):
                item = execution_result.data
                response_data = str_to_bytes(json.dumps({
                    'data': item
                }))
                yield Payload(response_data), False

            yield Payload(), True

        return StreamFromAsyncGenerator(generator)

    return router
