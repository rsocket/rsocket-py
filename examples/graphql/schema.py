import asyncio

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
        "subscriptionsTest": GraphQLField(
            type_=QueryRootType, resolve=lambda *args: QueryRootType
        )
    },
)

Schema = GraphQLSchema(QueryRootType, MutationRootType, SubscriptionsRootType)


# Schema with async methods
async def greeting(_obj, info):
    return "hello world"


async def echo(_obj, info, input):
    return input


def resolver_field_sync(_obj, info):
    return "hey3"


AsyncQueryType = GraphQLObjectType(
    "AsyncQueryType",
    {
        "greeting": GraphQLField(GraphQLString, resolve=greeting),
        "echo": GraphQLField(GraphQLString,
                             args={"input": GraphQLArgument(GraphQLString)},
                             resolve=echo),
        "c": GraphQLField(GraphQLString, resolve=resolver_field_sync),
    },
)


def resolver_field_sync_1(_obj, info):
    return "synced_one"


def resolver_field_sync_2(_obj, info):
    return "synced_two"


SyncQueryType = GraphQLObjectType(
    "SyncQueryType",
    {
        "a": GraphQLField(GraphQLString, resolve=resolver_field_sync_1),
        "b": GraphQLField(GraphQLString, resolve=resolver_field_sync_2),
    },
)

AsyncSchema = GraphQLSchema(AsyncQueryType)
SyncSchema = GraphQLSchema(SyncQueryType)
