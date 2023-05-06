import asyncio
import functools
import io
import json
import logging
from asyncio import Queue
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Tuple, Type

import aiohttp
from gql.transport import AsyncTransport
from graphql import DocumentNode, ExecutionResult, print_ast

from reactivestreams.subscriber import DefaultSubscriber
from ..extensions.helpers import composite, route
from ..frame_helpers import str_to_bytes
from ..payload import Payload
from ..rsocket_client import RSocketClient

log = logging.getLogger(__name__)


class RSocketTransport(AsyncTransport):

    def __init__(
            self,
            rsocket_client: RSocketClient,
            json_serialize: Callable = json.dumps,
    ):
        self._rsocket_client = rsocket_client
        self._json_serialize: Callable = json_serialize

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def execute(
            self,
            document: DocumentNode,
            variable_values: Optional[Dict[str, Any]] = None,
            operation_name: Optional[str] = None,
            extra_args: Dict[str, Any] = None,
            upload_files: bool = False,
    ) -> ExecutionResult:
        """
        Don't call this coroutine directly on the transport, instead use
        :code:`execute` on a client or a session.

        :param document: the parsed GraphQL request
        :param variable_values: An optional Dict of variable values
        :param operation_name: An optional Operation name for the request
        :param extra_args: additional arguments to send to the aiohttp post method
        :param upload_files: Set to True if you want to put files in the variable values
        :returns: an ExecutionResult object.
        """

        rsocket_payload = self._create_rsocket_payload(document, variable_values, operation_name)
        response = await self._rsocket_client.request_response(rsocket_payload)

        return self._response_to_execution_result(response)

    def _response_to_execution_result(self, response: Payload) -> ExecutionResult:
        result = json.loads(response.data.decode('utf-8'))

        return ExecutionResult(
            errors=result.get("errors"),
            data=result.get("data"),
            extensions=result.get("extensions"),
        )

    def _create_rsocket_payload(self, document, variable_values, operation_name):
        query_str = print_ast(document)

        payload: Dict[str, Any] = {
            "query": query_str,
        }

        if operation_name:
            payload["operationName"] = operation_name

        if variable_values:
            payload["variables"] = variable_values

        if log.isEnabledFor(logging.INFO):
            log.info(">>> %s", self._json_serialize(payload))

        rsocket_payload = Payload(str_to_bytes(self._json_serialize(payload)), composite(route('graphql')))

        return rsocket_payload

    async def subscribe(
            self,
            document: DocumentNode,
            variable_values: Optional[Dict[str, Any]] = None,
            operation_name: Optional[str] = None,
    ) -> AsyncGenerator[ExecutionResult, None]:
        """Subscribe is not supported on HTTP.

        :meta private:
        """

        complete_object = object()

        class StreamSubscriber(DefaultSubscriber):
            def __init__(self, _received_queue: Queue):
                super().__init__()
                self._received_queue = _received_queue

            def on_next(self, value: Payload, is_complete: bool = False):
                self._received_queue.put_nowait(value)

                if is_complete:
                    self._received_queue.put_nowait(complete_object)

            def on_complete(self):
                self._received_queue.put_nowait(complete_object)

        rsocket_payload = self._create_rsocket_payload(document, variable_values, operation_name)

        received_queue = Queue()
        subscriber = StreamSubscriber(received_queue)

        self._rsocket_client.request_stream(rsocket_payload).subscribe(subscriber)

        while True:
            response = await received_queue.get()

            if response is complete_object:
                break

            yield self._response_to_execution_result(response)
