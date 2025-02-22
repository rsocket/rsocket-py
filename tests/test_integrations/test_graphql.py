import asyncio

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


async def test_graphql_get_schema(lazy_pipe, graphql_schema):
    expected_schema = {'__schema': {
        'types': [{'name': 'Query'}, {'name': 'String'}, {'name': 'Subscription'}, {'name': 'Greeting'},
                  {'name': 'Mutation'}, {'name': 'Boolean'}, {'name': '__Schema'}, {'name': '__Type'},
                  {'name': '__TypeKind'}, {'name': '__Field'}, {'name': '__InputValue'}, {'name': '__EnumValue'},
                  {'name': '__Directive'}, {'name': '__DirectiveLocation'}]}}

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
            gql("""{__schema { types { name  }  } }"""),
            get_execution_result=True)

        assert response.data == expected_schema


async def test_graphql_break_loop(lazy_pipe, graphql_schema):
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
        i = 0
        async for response in graphql.subscribe_async(
                document=gql("""
                subscription {
                    greetings {message}
                }
                """),
                get_execution_result=True):
            responses.append(response.data)
            i += 1
            if i > 4:
                break

        assert len(responses) == 5
        assert responses[0] == {'greetings': {'message': 'Hello world 0'}}

        await asyncio.sleep(1)

        # assert responses[9] == {'greetings': {'message': 'Hello world 9'}}
