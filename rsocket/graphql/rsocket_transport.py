import asyncio
import functools
import io
import json
import logging
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Tuple, Type

import aiohttp
from gql.transport import AsyncTransport
from graphql import DocumentNode, ExecutionResult, print_ast

from ..extensions.helpers import composite, route
from ..frame_helpers import str_to_bytes
from ..payload import Payload
from ..rsocket_client import RSocketClient

log = logging.getLogger(__name__)


class RSocketTransport(AsyncTransport):
    """:ref:`Async Transport <async_transports>` to execute GraphQL queries
    on remote servers with an HTTP connection.

    This transport use the aiohttp library with asyncio.
    """

    file_classes: Tuple[Type[Any], ...] = (
        io.IOBase,
        aiohttp.StreamReader,
        AsyncGenerator,
    )

    def __init__(
            self,
            rsocket_client: RSocketClient,
            json_serialize: Callable = json.dumps,
    ):
        self._rsocket_client = rsocket_client
        self._json_serialize: Callable = json_serialize

    async def connect(self) -> None:
        pass

    @staticmethod
    def create_aiohttp_closed_event(session) -> asyncio.Event:
        """Work around aiohttp issue that doesn't properly close transports on exit.

        See https://github.com/aio-libs/aiohttp/issues/1925#issuecomment-639080209

        Returns:
           An event that will be set once all transports have been properly closed.
        """

        ssl_transports = 0
        all_is_lost = asyncio.Event()

        def connection_lost(exc, orig_lost):
            nonlocal ssl_transports

            try:
                orig_lost(exc)
            finally:
                ssl_transports -= 1
                if ssl_transports == 0:
                    all_is_lost.set()

        def eof_received(orig_eof_received):
            try:
                orig_eof_received()
            except AttributeError:  # pragma: no cover
                # It may happen that eof_received() is called after
                # _app_protocol and _transport are set to None.
                pass

        for conn in session.connector._conns.values():
            for handler, _ in conn:
                proto = getattr(handler.transport, "_ssl_protocol", None)
                if proto is None:
                    continue

                ssl_transports += 1
                orig_lost = proto.connection_lost
                orig_eof_received = proto.eof_received

                proto.connection_lost = functools.partial(
                    connection_lost, orig_lost=orig_lost
                )
                proto.eof_received = functools.partial(
                    eof_received, orig_eof_received=orig_eof_received
                )

        if ssl_transports == 0:
            all_is_lost.set()

        return all_is_lost

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
        """Execute the provided document AST against the configured remote server
        using the current session.
        This uses the aiohttp library to perform a HTTP POST request asynchronously
        to the remote server.

        Don't call this coroutine directly on the transport, instead use
        :code:`execute` on a client or a session.

        :param document: the parsed GraphQL request
        :param variable_values: An optional Dict of variable values
        :param operation_name: An optional Operation name for the request
        :param extra_args: additional arguments to send to the aiohttp post method
        :param upload_files: Set to True if you want to put files in the variable values
        :returns: an ExecutionResult object.
        """

        query_str = print_ast(document)

        payload: Dict[str, Any] = {
            "query": query_str,
        }

        if operation_name:
            payload["operationName"] = operation_name

        # if upload_files:
        #
        #     # If the upload_files flag is set, then we need variable_values
        #     assert variable_values is not None
        #
        #     # If we upload files, we will extract the files present in the
        #     # variable_values dict and replace them by null values
        #     nulled_variable_values, files = extract_files(
        #         variables=variable_values,
        #         file_classes=self.file_classes,
        #     )
        #
        #     # Save the nulled variable values in the payload
        #     payload["variables"] = nulled_variable_values
        #
        #     # Prepare aiohttp to send multipart-encoded data
        #     data = aiohttp.FormData()
        #
        #     # Generate the file map
        #     # path is nested in a list because the spec allows multiple pointers
        #     # to the same file. But we don't support that.
        #     # Will generate something like {"0": ["variables.file"]}
        #     file_map = {str(i): [path] for i, path in enumerate(files)}
        #
        #     # Enumerate the file streams
        #     # Will generate something like {'0': <_io.BufferedReader ...>}
        #     file_streams = {str(i): files[path] for i, path in enumerate(files)}
        #
        #     # Add the payload to the operations field
        #     operations_str = self._json_serialize(payload)
        #     log.debug("operations %s", operations_str)
        #     data.add_field(
        #         "operations", operations_str, content_type="application/json"
        #     )
        #
        #     # Add the file map field
        #     file_map_str = self._json_serialize(file_map)
        #     log.debug("file_map %s", file_map_str)
        #     data.add_field("map", file_map_str, content_type="application/json")
        #
        #     # Add the extracted files as remaining fields
        #     for k, f in file_streams.items():
        #         name = getattr(f, "name", k)
        #         content_type = getattr(f, "content_type", None)
        #
        #         data.add_field(k, f, filename=name, content_type=content_type)
        #
        #     post_args: Dict[str, Any] = {"data": data}
        #
        # else:
        #     if variable_values:
        #         payload["variables"] = variable_values
        #
        if log.isEnabledFor(logging.INFO):
            log.info(">>> %s", self._json_serialize(payload))
        #
        #     post_args = {"json": payload}

        rsocket_payload = Payload(str_to_bytes(self._json_serialize(payload)), composite(route('graphql')))
        response = await self._rsocket_client.request_response(rsocket_payload)

        result = json.loads(response.data.decode('utf-8'))

        return ExecutionResult(
            errors=result.get("errors"),
            data=result.get("data"),
            extensions=result.get("extensions"),
        )

        # async with self.session.post(self.url, ssl=self.ssl, **post_args) as resp:
        #
        #     # Saving latest response headers in the transport
        #     self.response_headers = resp.headers
        #
        #     async def raise_response_error(resp: aiohttp.ClientResponse, reason: str):
        #         # We raise a TransportServerError if the status code is 400 or higher
        #         # We raise a TransportProtocolError in the other cases
        #
        #         try:
        #             # Raise a ClientResponseError if response status is 400 or higher
        #             resp.raise_for_status()
        #         except ClientResponseError as e:
        #             raise TransportServerError(str(e), e.status) from e
        #
        #         result_text = await resp.text()
        #         raise TransportProtocolError(
        #             f"Server did not return a GraphQL result: "
        #             f"{reason}: "
        #             f"{result_text}"
        #         )
        #
        #     try:
        #         result = await resp.json(content_type=None)
        #
        #         if log.isEnabledFor(logging.INFO):
        #             result_text = await resp.text()
        #             log.info("<<< %s", result_text)
        #
        #     except Exception:
        #         await raise_response_error(resp, "Not a JSON answer")
        #
        #     if result is None:
        #         await raise_response_error(resp, "Not a JSON answer")
        #
        #     if "errors" not in result and "data" not in result:
        #         await raise_response_error(resp, 'No "data" or "errors" keys in answer')
        #
        #     return ExecutionResult(
        #         errors=result.get("errors"),
        #         data=result.get("data"),
        #         extensions=result.get("extensions"),
        #     )

    def subscribe(
            self,
            document: DocumentNode,
            variable_values: Optional[Dict[str, Any]] = None,
            operation_name: Optional[str] = None,
    ) -> AsyncGenerator[ExecutionResult, None]:
        """Subscribe is not supported on HTTP.

        :meta private:
        """
        raise NotImplementedError(" The HTTP transport does not support subscriptions")
