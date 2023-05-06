from graphql.type.definition import (
    GraphQLArgument,
    GraphQLField,
    GraphQLNonNull,
    GraphQLObjectType,
)
from graphql.type.scalars import GraphQLString
from graphql.type.schema import GraphQLSchema


def resolve_raises(*_):
    raise Exception("Throws!")


EchoType = GraphQLObjectType(
    name="Echo",
    fields={
        "message": GraphQLField(
            type_=GraphQLString,
            description="The message echoed back",
        )
    },
    description="The result of an echo request",
)

# Sync schema
QueryRootType = GraphQLObjectType(
    name="QueryRoot",
    fields={
        "thrower": GraphQLField(
            GraphQLNonNull(GraphQLString),
            resolve=resolve_raises,
        ),
        "request": GraphQLField(
            GraphQLNonNull(GraphQLString),
            resolve=lambda obj, info, *args: info.context["request"].query.get("q"),
        ),
        "context": GraphQLField(
            GraphQLObjectType(
                name="context",
                fields={
                    "session": GraphQLField(GraphQLString),
                    "request": GraphQLField(
                        GraphQLNonNull(GraphQLString),
                        resolve=lambda obj, info: info.context["request"],
                    ),
                    "property": GraphQLField(
                        GraphQLString, resolve=lambda obj, info: info.context.property
                    ),
                },
            ),
            resolve=lambda obj, info: info.context,
        ),
        "test": GraphQLField(
            type_=GraphQLString,
            args={"who": GraphQLArgument(GraphQLString)},
            resolve=lambda obj, info, who=None: "Hello %s" % (who or "World"),
        ),
    },
)


# Schema with async methods
async def resolve_greeting(_obj, info):
    return "hello world"


async def resolve_echo(_obj, info, input):
    return {'message': input}


async def resolve_multiple_greetings(_obj, info):
    for greeting in ['Hello', 'Hi', 'Yo']:
        yield greeting


AsyncQueryType = GraphQLObjectType(
    "AsyncQueryType",
    {
        "greeting": GraphQLField(
            GraphQLString,
            resolve=resolve_greeting),
        "echo": GraphQLField(
            EchoType,
            args={"input": GraphQLArgument(GraphQLString)},
            resolve=resolve_echo),
        "multipleGreetings": GraphQLField(
            GraphQLString,
            resolve=resolve_multiple_greetings),
    },
)

MutationRootType = GraphQLObjectType(
    name="MutationRoot",
    fields={
        "writeTest": GraphQLField(
            type_=QueryRootType, resolve=lambda *args: QueryRootType
        )
    },
)

SubscriptionsRootType = GraphQLObjectType(
    name="SubscriptionsRoot",
    fields={
        "multipleGreetings": GraphQLField(
            type_=GraphQLString, resolve=resolve_multiple_greetings
        )
    },
)

AsyncSchema = GraphQLSchema(AsyncQueryType, subscription=SubscriptionsRootType)
