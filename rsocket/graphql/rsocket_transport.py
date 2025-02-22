import json
import logging
from asyncio import Queue
from typing import Any, AsyncGenerator, Callable, Dict, Optional

from gql.transport import AsyncTransport
from graphql import DocumentNode, ExecutionResult, print_ast

from reactivestreams.subscriber import DefaultSubscriber
from rsocket.extensions.helpers import composite, route
from rsocket.frame_helpers import str_to_bytes
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient

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

    def _create_rsocket_payload(self,
                                document: DocumentNode,
                                variable_values: Optional[Dict[str, Any]],
                                operation_name: str) -> Payload:
        query_str = print_ast(document)

        payload: Dict[str, Any] = {
            "query": query_str,
        }

        if operation_name:
            payload["operationName"] = operation_name

        if variable_values:
            payload["variables"] = self._json_serialize(variable_values)

        payload_serialized = self._json_serialize(payload)

        if log.isEnabledFor(logging.INFO):
            log.info(">>> %s", payload_serialized)

        rsocket_payload = Payload(str_to_bytes(payload_serialized), composite(route('graphql')))

        return rsocket_payload

    async def subscribe(
            self,
            document: DocumentNode,
            variable_values: Optional[Dict[str, Any]] = None,
            operation_name: Optional[str] = None,
    ) -> AsyncGenerator[ExecutionResult, None]:

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

            def on_error(self, exception: Exception):
                self._received_queue.put_nowait(exception)

            def cancel(self):
                if self.subscription is not None:
                    self.subscription.cancel()

            def request(self, n: int):
                if self.subscription is not None:
                    self.subscription.request(n)

        rsocket_payload = self._create_rsocket_payload(document, variable_values, operation_name)

        received_queue = Queue()
        subscriber = StreamSubscriber(received_queue)

        self._rsocket_client.request_stream(rsocket_payload).subscribe(subscriber)

        while True:
            try:
                response = await received_queue.get()

                if isinstance(response, Exception):
                    raise response

                if response is complete_object:
                    break

                execution_result = self._response_to_execution_result(response)

                yield execution_result
            except GeneratorExit:
                logger().debug('Generator exited')
                subscriber.cancel()
                return

