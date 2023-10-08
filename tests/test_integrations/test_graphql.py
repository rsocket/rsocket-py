from gql import Client, gql

from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.graphql.rsocket_transport import RSocketTransport
from rsocket.graphql.server_helper import graphql_handler
from rsocket.routing.routing_request_handler import RoutingRequestHandler


async def test_graphql_query(lazy_pipe, graphql_schema):
    def handler_factory():
        return RoutingRequestHandler(graphql_handler(graphql_schema, 'graphql'))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        graphql = Client(
            schema=graphql_schema,
            transport=RSocketTransport(client),
        )

        response = await graphql.execute_async(
            gql("""query { greeting { message } }"""),
            get_execution_result=True)

        assert response.data['greeting']['message'] == 'Hello world'


async def test_graphql_subscription(lazy_pipe, graphql_schema):
    def handler_factory():
        return RoutingRequestHandler(graphql_handler(graphql_schema, 'graphql'))

    async with lazy_pipe(
            client_arguments={'metadata_encoding': WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA},
            server_arguments={'handler_factory': handler_factory}) as (server, client):
        graphql = Client(
            schema=graphql_schema,
            transport=RSocketTransport(client),
        )

        responses = []

        async for response in graphql.subscribe_async(
                document=gql("""
                subscription {
                    greetings {message}
                }
                """),
                get_execution_result=True):
            responses.append(response.data)

        assert len(responses) == 10
        assert responses[0] == {'greetings': {'message': 'Hello world 0'}}
        assert responses[9] == {'greetings': {'message': 'Hello world 9'}}
