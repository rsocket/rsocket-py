import json
from collections import namedtuple
from typing import AsyncGenerator, Tuple
from typing import Dict, Optional, Union

from graphql import execute, parse, GraphQLSchema, ExecutionResult
from graphql import subscribe
from graphql.pyutils import is_awaitable

from rsocket.frame_helpers import str_to_bytes
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator

__all__ = ['graphql_handler']

GraphQLParams = namedtuple("GraphQLParams", "query variables operation_name")


def graphql_handler(schema: GraphQLSchema, route: str,
                    json_serialize=json.dumps, json_deserialize=json.loads):
    router = RequestRouter()

    @router.response(route)
    async def graphql_query(payload: Payload):
        document, params = parse_payload(payload, json_deserialize)

        execution_result = execute(schema, document, variable_values=params.variables,
                                   operation_name=params.operation_name)

        if is_awaitable(execution_result):
            execution_result = await execution_result

        rsocket_payload = graphql_to_rsocket_payload(execution_result, json_serialize)

        return create_future(rsocket_payload)

    @router.stream(route)
    async def graphql_subscription(payload: Payload):
        async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
            document, params = parse_payload(payload, json_deserialize)

            subscription = subscribe(schema, document, operation_name=params.operation_name)

            if is_awaitable(subscription):
                subscription = await subscription

            async for execution_result in subscription:
                rsocket_payload = graphql_to_rsocket_payload(execution_result, json_serialize)
                yield rsocket_payload, False

            yield Payload(), True

        return StreamFromAsyncGenerator(generator)

    return router


def graphql_to_rsocket_payload(execution_result: ExecutionResult, json_serialize) -> Payload:
    return Payload(str_to_bytes(json_serialize({
        'data': execution_result.data
    })))


def parse_payload(payload, json_deserialize):
    data = json_deserialize(payload.data.decode('utf-8'))
    params = get_graphql_params(data, {})
    document = parse(params.query)
    return document, params


def get_graphql_params(data: Dict, query_data: Dict) -> GraphQLParams:
    query = data.get("query") or query_data.get("query")
    variables = data.get("variables") or query_data.get("variables")
    operation_name = data.get("operationName") or query_data.get("operationName")

    return GraphQLParams(query, load_json_variables(variables), operation_name)


def load_json_variables(variables: Optional[Union[str, Dict]]) -> Optional[Dict]:
    if variables and isinstance(variables, str):
        try:
            return json.loads(variables)
        except Exception:
            raise Exception("Variables are invalid JSON.")
