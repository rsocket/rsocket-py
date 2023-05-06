import json

from graphql import execute, parse, ExecutionResult
from graphql_server import get_graphql_params

from examples.graphql.schema import AsyncSchema
from rsocket.payload import Payload


async def execute_query_in_payload(payload: Payload) -> ExecutionResult:
    data = json.loads(payload.data.decode('utf-8'))
    params = get_graphql_params(data, {})
    schema = AsyncSchema
    document = parse(params.query)

    execution_result = await execute(
        schema,
        document,
        variable_values=params.variables,
        operation_name=params.operation_name
    )

    return execution_result
