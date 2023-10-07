import json
from collections import namedtuple
from typing import Dict, Optional, Union

from graphql import execute, parse, ExecutionResult, GraphQLSchema

from rsocket.payload import Payload

GraphQLParams = namedtuple("GraphQLParams", "query variables operation_name")


async def execute_query_in_payload(payload: Payload,
                                   schema: GraphQLSchema) -> ExecutionResult:
    data = json.loads(payload.data.decode('utf-8'))
    params = get_graphql_params(data, {})
    document = parse(params.query)

    execution_result = execute(
        schema,
        document,
        variable_values=params.variables,
        operation_name=params.operation_name
    )

    return execution_result


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
        return variables  # type: ignore
