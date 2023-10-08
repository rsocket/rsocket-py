import asyncio
from pathlib import Path
from typing import Dict

import pytest
from graphql import build_schema


@pytest.fixture
def graphql_schema():
    stored_message = ""

    async def greeting(*args) -> Dict:
        return {
            'message': "Hello world"
        }

    async def get_message(*args) -> str:
        nonlocal stored_message
        return stored_message

    async def set_message(root, _info, message) -> Dict:
        nonlocal stored_message
        stored_message = message
        return {
            "message": message
        }

    def greetings(*args):
        async def results():
            for i in range(10):
                yield {'greetings': {'message': f"Hello world {i}"}}
                await asyncio.sleep(0.01)

        return results()

    with (Path(__file__).parent / 'rsocket.graphqls').open() as fd:
        schema = build_schema(fd.read())

    schema.query_type.fields['greeting'].resolve = greeting
    schema.query_type.fields['getMessage'].resolve = get_message
    schema.mutation_type.fields['setMessage'].resolve = set_message
    schema.subscription_type.fields['greetings'].subscribe = greetings

    return schema
