import time
import os
import sys
from typing import Any, List, Optional, Type, Union

from cffi import FFI
import anyio
from glide.async_commands.cluster_commands import ClusterCommands
from glide.async_commands.core import CoreCommands
from glide.async_commands.standalone_commands import StandaloneCommands
from glide.config import BaseClientConfiguration, GlideClusterClientConfiguration
from glide.constants import OK, TEncodable, TResult
from glide.protobuf.command_request_pb2 import RequestType
from glide.routes import Route
from glide.protobuf.response_pb2 import RequestErrorType
from glide.exceptions import (
ClosingError,
ConnectionError,
ExecAbortError,
RequestError,
TimeoutError,
)
from anyio.from_thread import start_blocking_portal,  BlockingPortal
import threading

if sys.version_info >= (3, 11):
    from typing import Self
else:
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

class FFIClientTypeEnum:
    Async = 0
    Sync = 1
    
class _CompatFuture:
    def __init__(self) -> None:
        self._is_done = anyio.Event()
        self._result: Any = None
        self._exception: Optional[Exception] = None

    def set_result(self, result: Any) -> None:
        # print(f"Setting result at {time.time()} with value: {result}")
        self._result = result
        self._is_done.set()
        # print("Event set completed")

    def set_exception(self, exception: Exception) -> None:
        # print(f"Setting exception at {time.time()}: {exception}")
        self._exception = exception
        self._is_done.set()

    def done(self) -> bool:
        return self._is_done.is_set()

    def __await__(self):
        # print(f"Starting to wait at {time.time()}")
        return self._is_done.wait()

    def result(self) -> Any:
        # print("Getting result")
        if self._exception:
            # print(f"Raising exception: {self._exception}")
            raise self._exception
        # print(f"Returning result: {self._result}")
        return self._result


    
class BaseClient(CoreCommands):
    def __init__(self, config: BaseClientConfiguration):
        self.config: BaseClientConfiguration = config
        self._is_closed: bool = False

    @classmethod
    async def create(cls, config: BaseClientConfiguration) -> Self:
        self = cls(config)
        self._init_ffi()
        self.config = config
        self._is_closed = False
        self._available_callback_indexes: List[int] = list() 
        self._available_futures = {}
        self._result_events = {}  # Add this line
        self._portal = None

        
        @self.ffi.callback("void(size_t, const CommandResponse*)")
        def success_callback(index_ptr, message):
            # print(f"Success callback called with index: {index_ptr}")
            if message == self.ffi.NULL:
                # print("got NULL")
                return
            parsed_response = self._handle_response(message)
            # print(f"Parsed response: {parsed_response}")
            index_ptr_int = int(index_ptr)
            future = self._available_futures.get(index_ptr_int)
            
            # Check if portal exists and client is not closed
            if future and hasattr(self, '_portal') and self._portal and not self._is_closed:
                # print("Found future, setting result via portal")
                self._portal.call(future.set_result, parsed_response)
                # print("Result set scheduled")


        @self.ffi.callback("void(size_t, const char*, int)")
        def failure_callback(index_ptr, error_message, error_type):
            try:
                error_msg = self.ffi.string(error_message).decode("utf-8") if error_message != self.ffi.NULL else "Unknown Error"
                index_ptr = int(index_ptr)
                future = self._available_futures.get(index_ptr)
                if future and self._portal:
                    error_class = get_request_error_class(error_type)
                    error = error_class(error_msg)
                    self._portal.call(future.set_exception, error)
            except Exception as e:
                # print(f"Error in failure callback: {e}")
                pass

        self.success_callback = success_callback
        self.failure_callback = failure_callback

        # Create client_type with the correct type definition
        client_type = self.ffi.new(
            "ClientType*",
            {
                "_type": FFIClientTypeEnum.Async,
            }
        )

        # Cast the callbacks to the correct type before assignment
        success_cb_cast = self.ffi.cast(
            "void(*)(uintptr_t, const void*)", 
            success_callback
        )
        failure_cb_cast = self.ffi.cast(
            "void(*)(uintptr_t, const char*, int)",
            failure_callback
        )

        client_type.async_client.success_callback = success_cb_cast
        client_type.async_client.failure_callback = failure_cb_cast

        conn_req = config._create_a_protobuf_conn_request(
            cluster_mode=type(config) is GlideClusterClientConfiguration
        )
        conn_req_bytes = conn_req.SerializeToString()

        client_response_ptr = await anyio.to_thread.run_sync(
            self.lib.create_client,
            conn_req_bytes,
            len(conn_req_bytes),
            client_type
        )

        if client_response_ptr != self.ffi.NULL:
            client_response = self.ffi.cast("ConnectionResponse*", client_response_ptr)
            if client_response.conn_ptr != self.ffi.NULL:
                self.core_client = client_response.conn_ptr
            else:
                error_message = (
                    self.ffi.string(client_response.connection_error_message).decode(
                        "utf-8"
                    )
                    if client_response.connection_error_message != self.ffi.NULL
                    else "Unknown error"
                )
                raise ClosingError(error_message)
            self.lib.free_connection_response(client_response_ptr)
        else:
            raise ClosingError("Failed to create client, response pointer is NULL.")
        
        return self
    
    def _get_callback_index(self) -> int:
        try:
            return self._available_callback_indexes.pop()
        except IndexError:
            # The list is empty
            return len(self._available_futures)
    
    def _get_future(self, callback_idx: int) -> _CompatFuture:
        response_future: _CompatFuture = _CompatFuture()
        self._available_futures.update({callback_idx: response_future})
        return response_future
        
    async def _resolve_future(self, index_ptr, parsed_response):
        future = self._available_futures.get(index_ptr)
        if future:
            future.set_result(parsed_response)

    
    def _init_ffi(self):
        self.ffi = FFI()
        self.ffi.cdef("""
            struct CommandResponse {
                int response_type;
                long int_value;
                double float_value;
                bool bool_value;
                char* string_value;
                long string_value_len;
                struct CommandResponse* array_value;
                long array_value_len;
                struct CommandResponse* map_key;
                struct CommandResponse* map_value;
                struct CommandResponse* sets_value;
                long sets_value_len;
            };

            typedef struct CommandResponse CommandResponse;

            typedef enum {
                Null = 0,
                Int = 1,
                Float = 2,
                Bool = 3,
                String = 4,
                Array = 5,
                Map = 6,
                Sets = 7
            } ResponseType;

            typedef void (*SuccessCallback)(uintptr_t index_ptr, const CommandResponse* message);
            typedef void (*FailureCallback)(uintptr_t index_ptr, const char* error_message, int error_type);

            typedef struct {
                const void* conn_ptr;
                const char* connection_error_message;
            } ConnectionResponse;

            typedef struct {
                const char* command_error_message;
                int command_error_type;
            } CommandError;

            typedef struct {
                CommandResponse* response;
                CommandError* command_error;
            } CommandResult;

            typedef enum {
                Async = 0,
                Sync = 1
            } ClientTypeEnum;

            typedef struct {
                SuccessCallback success_callback;
                FailureCallback failure_callback;
            } AsyncClient;

            typedef struct {
                int _type;
                union {
                    struct {
                        void (*success_callback)(uintptr_t, const void*);
                        void (*failure_callback)(uintptr_t, const char*, int);
                    } async_client;
                };
            } ClientType;

            const ConnectionResponse* create_client(
                const uint8_t* connection_request_bytes,
                size_t connection_request_len,
                const ClientType* client_type
            );
            void close_client(const void* client_adapter_ptr);
            void free_connection_response(ConnectionResponse* connection_response_ptr);
            char* get_response_type_string(int response_type);
            void free_response_type_string(char* response_string);
            void free_command_response(CommandResponse* command_response_ptr);
            void free_error_message(char* error_message);
            void free_command_result(CommandResult* command_result_ptr);
            CommandResult* command(
                const void* client_adapter_ptr, uintptr_t channel, int command_type,
                unsigned long arg_count, const size_t *args, const unsigned long* args_len,
                const unsigned char* route_bytes, size_t route_bytes_len
            );
        """)

        this_dir = os.path.dirname(__file__)
        so_path = os.path.abspath(
            os.path.join(this_dir, "../../../ffi/target/debug/libglide_ffi.so")
        )
        self.lib = self.ffi.dlopen(so_path)

    def _handle_response(self, message):
        if message == self.ffi.NULL:
            # print("Received NULL message.")
            return None

        message_type = self.ffi.typeof(message).cname
        if message_type == "CommandResponse *":
            message = message[0]
            message_type = self.ffi.typeof(message).cname

        if message_type != "CommandResponse":
            raise RequestError(f"Unexpected message type = {message_type}")

        return self._handle_command_response(message)

    def _handle_command_response(self, msg):
        handlers = {
            0: self._handle_null_response,
            1: self._handle_int_response,
            2: self._handle_float_response,
            3: self._handle_bool_response,
            4: self._handle_string_response,
            5: self._handle_array_response,
            6: self._handle_map_response,
            7: self._handle_set_response,
            8: self._handle_ok_response,
        }
        handler = handlers.get(msg.response_type)
        if handler is None:
            raise RequestError(f"Unhandled response type = {msg.response_type}")
        return handler(msg)

    def _handle_null_response(self, msg):
        return None

    def _handle_int_response(self, msg):
        return msg.int_value

    def _handle_float_response(self, msg):
        return msg.float_value

    def _handle_bool_response(self, msg):
        return bool(msg.bool_value)

    def _handle_string_response(self, msg):
        try:
            return self.ffi.buffer(msg.string_value, msg.string_value_len)[:]
        except Exception as e:
            raise RequestError(f"Error decoding string value: {e}")

    def _handle_array_response(self, msg):
        array = []
        for i in range(msg.array_value_len):
            element = self.ffi.cast("struct CommandResponse*", msg.array_value + i)
            array.append(self._handle_response(element))
        return array

    def _handle_map_response(self, msg):
        map_dict = {}
        for i in range(msg.array_value_len):
            element = self.ffi.cast("struct CommandResponse*", msg.array_value + i)
            key = self.ffi.cast("struct CommandResponse*", element.map_key)
            value = self.ffi.cast("struct CommandResponse*", element.map_value)
            map_dict[self._handle_response(key)] = self._handle_response(value)
        return map_dict

    def _handle_set_response(self, msg):
        result_set = set()
        sets_array = self.ffi.cast(
            f"struct CommandResponse[{msg.sets_value_len}]", msg.sets_value
        )
        for i in range(msg.sets_value_len):
            element = sets_array[i]
            result_set.add(self._handle_response(element))
        return result_set

    def _handle_ok_response(self, msg):
        return OK

    def _to_c_strings(self, args):
        c_strings = []
        string_lengths = []
        buffers = []
        for arg in args:
            if isinstance(arg, str):
                arg_bytes = arg.encode("utf-8")
            elif isinstance(arg, (int, float)):
                arg_bytes = str(arg).encode("utf-8")
            elif isinstance(arg, bytes):
                arg_bytes = arg
            else:
                raise ValueError(f"Unsupported argument type: {type(arg)}")
            buffers.append(arg_bytes)
            c_strings.append(self.ffi.cast("size_t", self.ffi.from_buffer(arg_bytes)))
            string_lengths.append(len(arg_bytes))
        return (
            self.ffi.new("size_t[]", c_strings),
            self.ffi.new("unsigned long[]", string_lengths),
            buffers,
        )

    def _handle_cmd_result(self, command_result):
        try:
            if command_result == self.ffi.NULL:
                raise ClosingError("Internal error: Received NULL as a command result")
            if command_result.command_error != self.ffi.NULL:
                error = self.ffi.cast("CommandError*", command_result.command_error)
                error_message = self.ffi.string(error.command_error_message).decode("utf-8")
                error_class = get_request_error_class(error.command_error_type)
                raise error_class(error_message)
            else:
                return self._handle_response(command_result.response)
        finally:
            self.lib.free_command_result(command_result)

    async def _execute_command(
        self,
        request_type: RequestType.ValueType,
        args: List[TEncodable],
        route: Optional[Route] = None,
    ) -> TResult:
            if self._is_closed:
                raise ClosingError(
                    "Unable to execute requests; the client is closed. Please create a new client."
                )
            client_adapter_ptr = self.core_client
            if client_adapter_ptr == self.ffi.NULL:
                raise ValueError("Invalid client pointer.")
            
            callback_idx = self._get_callback_index()
            # print(f"Got callback_idx: {callback_idx}")
            response_future = self._get_future(callback_idx)
            
            try:
                # print("Creating C args")
                c_args, c_lengths, buffers = self._to_c_strings(args)
                route_bytes = b""
                route_ptr = self.ffi.NULL
                
                async with BlockingPortal() as portal:
                    self._portal = portal

                    # No need for portal here - the callback will set the result directly
                    self.lib.command(
                        client_adapter_ptr,
                        callback_idx,
                        request_type,
                        len(args),
                        c_args,
                        c_lengths,
                        route_ptr,
                        len(route_bytes),
                    )

                    # print("Waiting for command completion")
                    # Wait for the event instead of the future    
                    await response_future._is_done.wait()        
                    return response_future.result()

            finally:
                # Clean up
                # print(f"Cleaning up callback_idx: {callback_idx}")
                self._available_futures.pop(callback_idx, None)
                self._result_events.pop(callback_idx, None)
                self._available_callback_indexes.append(callback_idx)
                del buffers

            
    async def close(self, err_message: Optional[str] = None) -> None:
        # Make sure to close the portal when closing the client
        # if self._portal:
        #     await self._portal.close()
        self._is_closed = True


            
class GlideClusterClient(BaseClient, ClusterCommands):
    pass

class GlideClient(BaseClient, StandaloneCommands):
    pass

TGlideClient = Union[GlideClient, GlideClusterClient]
