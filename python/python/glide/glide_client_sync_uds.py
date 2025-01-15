# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

import asyncio
import sys
import threading
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast
import socket
import time

from glide.async_commands.cluster_commands import ClusterCommands
from glide.async_commands.command_args import ObjectType
from glide.sync_commands.core import CoreCommands
from glide.async_commands.standalone_commands import StandaloneCommands
from glide.config import BaseClientConfiguration, ServerCredentials
from glide.constants import DEFAULT_READ_BYTES_SIZE, OK, TEncodable, TRequest, TResult
from glide.exceptions import (
    ClosingError,
    ConfigurationError,
    ConnectionError,
    ExecAbortError,
    RequestError,
    TimeoutError,
)
from glide.logger import Level as LogLevel
from glide.logger import Logger as ClientLogger
from glide.protobuf.command_request_pb2 import Command, CommandRequest, RequestType
from glide.protobuf.connection_request_pb2 import ConnectionRequest
from glide.protobuf.response_pb2 import RequestErrorType, Response
from glide.protobuf_codec import PartialMessageException, ProtobufCodec
from glide.routes import Route, set_protobuf_route

from .glide import (
    DEFAULT_TIMEOUT_IN_MILLISECONDS,
    MAX_REQUEST_ARGS_LEN,
    ClusterScanCursor,
    create_leaked_bytes_vec,
    get_statistics,
    start_socket_listener_external,
    value_from_pointer,
)

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
    from typing import Self
else:
    import async_timeout
    from typing_extensions import Self


def get_request_error_class(
    error_type: Optional[RequestErrorType.ValueType],
) -> Type[RequestError]:
    if error_type == RequestErrorType.Disconnect:
        return ConnectionError
    if error_type == RequestErrorType.ExecAbort:
        return ExecAbortError
    if error_type == RequestErrorType.Timeout:
        return TimeoutError
    if error_type == RequestErrorType.Unspecified:
        return RequestError
    return RequestError


class UDSBaseClient(CoreCommands):
    def __init__(self, config: BaseClientConfiguration):
        """
        To create a new client, use the `create` classmethod
        """
        self.config: BaseClientConfiguration = config
        self._available_futures: Dict[int, asyncio.Future] = {}
        self._available_callback_indexes: List[int] = list()
        self._buffered_requests: List[TRequest] = list()
        self._writer_lock = threading.Lock()
        self.socket_path: Optional[str] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._is_closed: bool = False
        self._pubsub_futures: List[asyncio.Future] = []
        self._pubsub_lock = threading.Lock()
        self._pending_push_notifications: List[Response] = list()

    @classmethod
    def create(cls, config: BaseClientConfiguration) -> Self:
        """Creates a Glide client.

        Args:
            config (ClientConfiguration): The client configurations.
                If no configuration is provided, a default client to "localhost":6379 will be created.

        Returns:
            Self: a Glide Client instance.
        """
        config = config
        self = cls(config)

        def init_callback(socket_path: Optional[str], err: Optional[str]):
            nonlocal self
            if err is not None:
                raise ClosingError(err)
            elif socket_path is None:
                raise ClosingError(
                    "Socket initialization error: Missing valid socket path."
                )
            else:
                # Received socket path
                print("caliing notify")
                self.socket_path = socket_path
        self.socket_path = start_socket_listener_external(init_callback=init_callback)

        # will log if the logger was created (wrapper or costumer) on info
        # level or higher
        # Wait for the socket listener to complete its initialization
        # Wait for the socket listener to complete its initialization
        while True:  # Loop to handle spurious wakeups
            ClientLogger.log(LogLevel.INFO, "connection info", "new connection established")
            # Create UDS connection
            try:
                self._create_uds_connection()
                print("socket is ready!")
            except Exception as e:
                print(f"got ex: {e}")
                time.sleep(1)
                continue
            # Set the client configurations
            self._set_connection_configurations()
            return self

    def _create_uds_connection(self) -> None:
        try:
            # Open a UDS connection
            self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._socket.settimeout(DEFAULT_TIMEOUT_IN_MILLISECONDS)
            self._socket.connect(self.socket_path)
            print("UDS connection created successfully.")
        except Exception as e:
            self.close()
            raise RuntimeError(f"Failed to create UDS connection: {e}") from e

    def __del__(self) -> None:
        try:
            if self._reader_task:
                self._reader_task.cancel()
        except RuntimeError as e:
            if "no running event loop" in str(e):
                # event loop already closed
                pass

    def close(self, err_message: Optional[str] = None) -> None:
        """
        Terminate the client by closing all associated resources, including the socket and any active futures.
        All open futures will be closed with an exception.

        Args:
            err_message (Optional[str]): If not None, this error message will be passed along with the exceptions when closing all open futures.
            Defaults to None.
        """
        self._is_closed = True
        for response_future in self._available_futures.values():
            if not response_future.done():
                err_message = "" if err_message is None else err_message
                response_future.set_exception(ClosingError(err_message))
        try:
            self._pubsub_lock.acquire()
            for pubsub_future in self._pubsub_futures:
                if not pubsub_future.done() and not pubsub_future.cancelled():
                    pubsub_future.set_exception(ClosingError(""))
        finally:
            self._pubsub_lock.release()

        self._socket.close()
        self.__del__()

    def _get_future(self, callback_idx: int) -> asyncio.Future:
        response_future: asyncio.Future = asyncio.Future()
        self._available_futures.update({callback_idx: response_future})
        return response_future

    def _get_protobuf_conn_request(self) -> ConnectionRequest:
        return self.config._create_a_protobuf_conn_request()

    def _set_connection_configurations(self) -> None:
        conn_request = self._get_protobuf_conn_request()
        print(f"conn_request= {conn_request}")
        self._write_or_buffer_request(conn_request)
        result = self._read_response()
        if result is not OK:
            raise ClosingError(result)

    def _create_write_task(self, request: TRequest):
        self._write_or_buffer_request(request)

    def _write_or_buffer_request(self, request: TRequest):
        self._buffered_requests.append(request)
        if self._writer_lock.acquire(False):
            try:
                while len(self._buffered_requests) > 0:
                    return self._write_buffered_requests_to_socket()

            finally:
                self._writer_lock.release()

    def _write_buffered_requests_to_socket(self) -> None:
        requests = self._buffered_requests
        self._buffered_requests = list()
        b_arr = bytearray()
        for request in requests:
            ProtobufCodec.encode_delimited(b_arr, request)
        self._socket.sendall(b_arr)

    def _encode_arg(self, arg: TEncodable) -> bytes:
        """
        Converts a string argument to bytes.

        Args:
            arg (str): An encodable argument.

        Returns:
            bytes: The encoded argument as bytes.
        """
        if isinstance(arg, str):
            # TODO: Allow passing different encoding options
            return bytes(arg, encoding="utf8")
        return arg

    def _encode_and_sum_size(
        self,
        args_list: Optional[List[TEncodable]],
    ) -> Tuple[List[bytes], int]:
        """
        Encodes the list and calculates the total memory size.

        Args:
            args_list (Optional[List[TEncodable]]): A list of strings to be converted to bytes.
                                                           If None or empty, returns ([], 0).

        Returns:
            int: The total memory size of the encoded arguments in bytes.
        """
        args_size = 0
        encoded_args_list: List[bytes] = []
        if not args_list:
            return (encoded_args_list, args_size)
        for arg in args_list:
            encoded_arg = self._encode_arg(arg) if isinstance(arg, str) else arg
            encoded_args_list.append(encoded_arg)
            args_size += sys.getsizeof(encoded_arg)
        return (encoded_args_list, args_size)

    def _execute_command(
        self,
        request_type: RequestType.ValueType,
        args: List[TEncodable],
        route: Optional[Route] = None,
    ) -> TResult:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )
        request = CommandRequest()
        request.callback_idx = self._get_callback_index()
        request.single_command.request_type = request_type
        request.single_command.args_array.args[:] = [
            bytes(elem, encoding="utf8") if isinstance(elem, str) else elem
            for elem in args
        ]
        (encoded_args, args_size) = self._encode_and_sum_size(args)
        if args_size < MAX_REQUEST_ARGS_LEN:
            request.single_command.args_array.args[:] = encoded_args
        else:
            request.single_command.args_vec_pointer = create_leaked_bytes_vec(
                encoded_args
            )
        set_protobuf_route(request, route)
        return self._write_request_await_response(request)

    async def _execute_transaction(
        self,
        commands: List[Tuple[RequestType.ValueType, List[TEncodable]]],
        route: Optional[Route] = None,
    ) -> List[TResult]:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )
        request = CommandRequest()
        request.callback_idx = self._get_callback_index()
        transaction_commands = []
        for requst_type, args in commands:
            command = Command()
            command.request_type = requst_type
            # For now, we allow the user to pass the command as array of strings
            # we convert them here into bytes (the datatype that our rust core expects)
            (encoded_args, args_size) = self._encode_and_sum_size(args)
            if args_size < MAX_REQUEST_ARGS_LEN:
                command.args_array.args[:] = encoded_args
            else:
                command.args_vec_pointer = create_leaked_bytes_vec(encoded_args)
            transaction_commands.append(command)
        request.transaction.commands.extend(transaction_commands)
        set_protobuf_route(request, route)
        return self._write_request_await_response(request)

    async def _execute_script(
        self,
        hash: str,
        keys: Optional[List[Union[str, bytes]]] = None,
        args: Optional[List[Union[str, bytes]]] = None,
        route: Optional[Route] = None,
    ) -> TResult:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )
        request = CommandRequest()
        request.callback_idx = self._get_callback_index()
        (encoded_keys, keys_size) = self._encode_and_sum_size(keys)
        (encoded_args, args_size) = self._encode_and_sum_size(args)
        if (keys_size + args_size) < MAX_REQUEST_ARGS_LEN:
            request.script_invocation.hash = hash
            request.script_invocation.keys[:] = encoded_keys
            request.script_invocation.args[:] = encoded_args

        else:
            request.script_invocation_pointers.hash = hash
            request.script_invocation_pointers.keys_pointer = create_leaked_bytes_vec(
                encoded_keys
            )
            request.script_invocation_pointers.args_pointer = create_leaked_bytes_vec(
                encoded_args
            )
        set_protobuf_route(request, route)
        return self._write_request_await_response(request)

    async def get_pubsub_message(self) -> CoreCommands.PubSubMsg:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        if not self.config._is_pubsub_configured():
            raise ConfigurationError(
                "The operation will never complete since there was no pubsub subscriptions applied to the client."
            )

        if self.config._get_pubsub_callback_and_context()[0] is not None:
            raise ConfigurationError(
                "The operation will never complete since messages will be passed to the configured callback."
            )

        # locking might not be required
        response_future: asyncio.Future = asyncio.Future()
        try:
            self._pubsub_lock.acquire()
            self._pubsub_futures.append(response_future)
            self._complete_pubsub_futures_safe()
        finally:
            self._pubsub_lock.release()
        return await response_future

    def try_get_pubsub_message(self) -> Optional[CoreCommands.PubSubMsg]:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        if not self.config._is_pubsub_configured():
            raise ConfigurationError(
                "The operation will never succeed since there was no pubsbub subscriptions applied to the client."
            )

        if self.config._get_pubsub_callback_and_context()[0] is not None:
            raise ConfigurationError(
                "The operation will never succeed since messages will be passed to the configured callback."
            )

        # locking might not be required
        msg: Optional[CoreCommands.PubSubMsg] = None
        try:
            self._pubsub_lock.acquire()
            self._complete_pubsub_futures_safe()
            while len(self._pending_push_notifications) and not msg:
                push_notification = self._pending_push_notifications.pop(0)
                msg = self._notification_to_pubsub_message_safe(push_notification)
        finally:
            self._pubsub_lock.release()
        return msg

    def _cancel_pubsub_futures_with_exception_safe(self, exception: ConnectionError):
        while len(self._pubsub_futures):
            next_future = self._pubsub_futures.pop(0)
            if not next_future.cancelled():
                next_future.set_exception(exception)

    def _notification_to_pubsub_message_safe(
        self, response: Response
    ) -> Optional[CoreCommands.PubSubMsg]:
        pubsub_message = None
        push_notification = cast(
            Dict[str, Any], value_from_pointer(response.resp_pointer)
        )
        message_kind = push_notification["kind"]
        if message_kind == "Disconnection":
            ClientLogger.log(
                LogLevel.WARN,
                "disconnect notification",
                "Transport disconnected, messages might be lost",
            )
        elif (
            message_kind == "Message"
            or message_kind == "PMessage"
            or message_kind == "SMessage"
        ):
            values: List = push_notification["values"]
            if message_kind == "PMessage":
                pubsub_message = UDSBaseClient.PubSubMsg(
                    message=values[2], channel=values[1], pattern=values[0]
                )
            else:
                pubsub_message = UDSBaseClient.PubSubMsg(
                    message=values[1], channel=values[0], pattern=None
                )
        elif (
            message_kind == "PSubscribe"
            or message_kind == "Subscribe"
            or message_kind == "SSubscribe"
            or message_kind == "Unsubscribe"
            or message_kind == "PUnsubscribe"
            or message_kind == "SUnsubscribe"
        ):
            pass
        else:
            ClientLogger.log(
                LogLevel.WARN,
                "unknown notification",
                f"Unknown notification message: '{message_kind}'",
            )

        return pubsub_message

    def _complete_pubsub_futures_safe(self):
        while len(self._pending_push_notifications) and len(self._pubsub_futures):
            next_push_notification = self._pending_push_notifications.pop(0)
            pubsub_message = self._notification_to_pubsub_message_safe(
                next_push_notification
            )
            if pubsub_message:
                self._pubsub_futures.pop(0).set_result(pubsub_message)

    def _write_request_await_response(self, request: CommandRequest):
        # Create a response future for this request and add it to the available
        # futures map
        self._create_write_task(request)
        return self._read_response()

    def _get_callback_index(self) -> int:
        try:
            return self._available_callback_indexes.pop()
        except IndexError:
            # The list is empty
            return len(self._available_futures)

    def _process_response(self, response: Response) -> None:
        if response.HasField("closing_error"):
            err_msg = (
                response.closing_error
                if response.HasField("closing_error")
                else f"Client Error - closing due to unknown error. callback index:  {response.callback_idx}"
            )
            self.close(err_msg)
            raise ClosingError(err_msg)
        else:
            self._available_callback_indexes.append(response.callback_idx)
            if response.HasField("request_error"):
                error_type = get_request_error_class(response.request_error.type)
                raise error_type(response.request_error.message)
            elif response.HasField("resp_pointer"):
                return value_from_pointer(response.resp_pointer)
            elif response.HasField("constant_response"):
                return OK
            else:
                return None

    def _process_push(self, response: Response) -> None:
        if response.HasField("closing_error") or not response.HasField("resp_pointer"):
            err_msg = (
                response.closing_error
                if response.HasField("closing_error")
                else "Client Error - push notification without resp_pointer"
            )
            self.close(err_msg)
            raise ClosingError(err_msg)

        try:
            self._pubsub_lock.acquire()
            callback, context = self.config._get_pubsub_callback_and_context()
            if callback:
                pubsub_message = self._notification_to_pubsub_message_safe(response)
                if pubsub_message:
                    callback(pubsub_message, context)
            else:
                self._pending_push_notifications.append(response)
                self._complete_pubsub_futures_safe()
        finally:
            self._pubsub_lock.release()

    def _read_response(self) -> None:
        # Socket reader loop
        remaining_read_bytes = bytearray()
        while True:
            read_bytes = self._socket.recv(DEFAULT_READ_BYTES_SIZE)
            if len(read_bytes) == 0:
                err_msg = "The communication layer was unexpectedly closed."
                self.close(err_msg)
                raise ClosingError(err_msg)
            read_bytes = remaining_read_bytes + bytearray(read_bytes)
            read_bytes_view = memoryview(read_bytes)
            offset = 0
            while offset <= len(read_bytes):
                try:
                    response, offset = ProtobufCodec.decode_delimited(
                        read_bytes, read_bytes_view, offset, Response
                    )
                except PartialMessageException:
                    # Received only partial response, break the inner loop
                    remaining_read_bytes = read_bytes[offset:]
                    break
                response = cast(Response, response)
                if response.is_push:
                    return self._process_push(response=response)
                else:
                    return self._process_response(response=response)

    async def get_statistics(self) -> dict:
        return get_statistics()

    async def _update_connection_password(
        self, password: Optional[str], immediate_auth: bool
    ) -> TResult:
        request = CommandRequest()
        request.callback_idx = self._get_callback_index()
        if password is not None:
            request.update_connection_password.password = password
        request.update_connection_password.immediate_auth = immediate_auth
        response = self._write_request_await_response(request)
        # Update the client binding side password if managed to change core configuration password
        if response is OK:
            if self.config.credentials is None:
                self.config.credentials = ServerCredentials(password=password or "")
                self.config.credentials.password = password or ""
        return response


class UDSGlideClusterClientSync(UDSBaseClient, ClusterCommands):
    """
    Client used for connection to cluster servers.
    For full documentation, see
    https://github.com/valkey-io/valkey-glide/wiki/Python-wrapper#cluster
    """

    async def _cluster_scan(
        self,
        cursor: ClusterScanCursor,
        match: Optional[TEncodable] = None,
        count: Optional[int] = None,
        type: Optional[ObjectType] = None,
        allow_non_covered_slots: bool = False,
    ) -> List[Union[ClusterScanCursor, List[bytes]]]:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )
        request = CommandRequest()
        request.callback_idx = self._get_callback_index()
        # Take out the id string from the wrapping object
        cursor_string = cursor.get_cursor()
        request.cluster_scan.cursor = cursor_string
        request.cluster_scan.allow_non_covered_slots = allow_non_covered_slots
        if match is not None:
            request.cluster_scan.match_pattern = (
                self._encode_arg(match) if isinstance(match, str) else match
            )
        if count is not None:
            request.cluster_scan.count = count
        if type is not None:
            request.cluster_scan.object_type = type.value
        response = self._write_request_await_response(request)
        return [ClusterScanCursor(bytes(response[0]).decode()), response[1]]

    def _get_protobuf_conn_request(self) -> ConnectionRequest:
        return self.config._create_a_protobuf_conn_request(cluster_mode=True)


class UDSGlideClientSync(UDSBaseClient, StandaloneCommands):
    """
    Client used for connection to standalone servers.
    For full documentation, see
    https://github.com/valkey-io/valkey-glide/wiki/Python-wrapper#standalone
    """


TGlideClient = Union[UDSGlideClientSync, UDSGlideClusterClientSync]
