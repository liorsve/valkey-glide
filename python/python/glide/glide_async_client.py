import asyncio
from cffi import FFI
from glide.protobuf.command_request_pb2 import Command, CommandRequest, RequestType
from typing import List, Union, Optional
from glide.async_commands.core import CoreCommands
from glide.constants import DEFAULT_READ_BYTES_SIZE, OK, TEncodable, TRequest, TResult
from glide.routes import Route
from glide.config import GlideClusterClientConfiguration, NodeAddress, GlideClientConfiguration

class GlideAsync(CoreCommands): 
    
    def create_client(self, connection_request_bytes):
        request_len = len(connection_request_bytes)
        connection_response = self.lib.create_client(
            self.ffi.from_buffer(connection_request_bytes),
            request_len,
            self.success_callback,
            self.failure_callback
        )
        return connection_response

    def _get_callback_index(self) -> int:
        try:
            return self._available_callback_indexes.pop()
        except IndexError:
            # The list is empty
            return len(self._available_futures)

    def _get_future(self, callback_idx: int) -> asyncio.Future:
        response_future: asyncio.Future = asyncio.Future()
        self._available_futures.update({callback_idx: response_future})
        return response_future
    
    def _resolve_future(self, index_ptr, parsed_response):
        future = self._available_futures.get(index_ptr)
        if future:
            future.set_result(parsed_response)      
                 
    def __init__(self, config = None):
        self._init_ffi()
        self._available_callback_indexes: List[int] = list()
        self.loop = asyncio.get_event_loop()

        # Define success and failure callbacks
        @self.ffi.callback("void(size_t, const CommandResponse*)")
        def success_callback(index_ptr, message):
            if message == self.ffi.NULL:
                print("No message provided in success callback.")
            else:
                parsed_response = self._handle_response(message)
                index_ptr = int(index_ptr)  # Ensure index_ptr is an integer
                future = self._available_futures.get(index_ptr)
                if future:
                    self.loop.call_soon_threadsafe(
                        self._resolve_future, index_ptr, parsed_response
                    )
                else:
                    print(f"No future found for index: {index_ptr}")


        @self.ffi.callback("void(size_t, const char*, int)")
        def failure_callback(index_ptr, error_message, error_type):
            error_msg = self.ffi.string(error_message).decode("utf-8") if error_message != self.ffi.NULL else "Unknown Error"
            print(f"Failure callback called with index: {index_ptr}, error: {error_msg}, type: {error_type}")

        self.success_callback = success_callback
        self.failure_callback = failure_callback
        # Call the `create_client` function
        # config = GlideClusterClientConfiguration(NodeAddress("localhost", 6379))
        config = GlideClientConfiguration([NodeAddress("localhost", 6379)]) if config is None else config
        conn_req = config._create_a_protobuf_conn_request(cluster_mode=type(config) == GlideClusterClientConfiguration)
        conn_req_bytes = conn_req.SerializeToString()
        client_response_ptr = self.create_client(conn_req_bytes)
        # Handle the connection response
        if client_response_ptr != self.ffi.NULL:
            client_response = self.ffi.cast("ConnectionResponse*", client_response_ptr)
            if client_response.conn_ptr != self.ffi.NULL:
                print("Client created successfully.")
                self.core_client = client_response.conn_ptr
            else:
                error_message = self.ffi.string(client_response.connection_error_message).decode('utf-8') if client_response.connection_error_message != self.ffi.NULL else "Unknown error"
                print(f"Failed to create client. Error: {error_message}")

            # Free the connection response to avoid memory leaks
            self.lib.free_connection_response(client_response_ptr)
        else:
            print("Failed to create client, response pointer is NULL.")
        self._available_futures = {}

    def _init_ffi(self):
        self.ffi = FFI()

        # Define the CommandResponse struct and related types
        self.ffi.cdef("""
        typedef struct CommandResponse {
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
        } CommandResponse;

        typedef struct ConnectionResponse {
            const void* conn_ptr;
            const char* connection_error_message;
        } ConnectionResponse;

        typedef void (*SuccessCallback)(size_t index_ptr, const CommandResponse* message);
        typedef void (*FailureCallback)(size_t index_ptr, const char* error_message, int error_type);

        const ConnectionResponse* create_client(
            const uint8_t* connection_request_bytes,
            size_t connection_request_len,
            SuccessCallback success_callback,
            FailureCallback failure_callback
        );

        void free_command_response(CommandResponse* response);
        void free_connection_response(ConnectionResponse* response);

        void command(
            const void* client_adapter_ptr,
            size_t channel,
            int command_type,
            unsigned long arg_count,
            const size_t* args,
            const unsigned long* args_len,
            const uint8_t* route_bytes,
            size_t route_bytes_len
        );
        """)

        # Load the shared library (adjust the path to your compiled Rust library)
        self.lib = self.ffi.dlopen("/home/ubuntu/glide-for-redis/go/target/release/libglide_rs.so")
        # debug
        # self.lib = self.ffi.dlopen("/home/ubuntu/glide-for-redis/go/target/debug/libglide_rs.so")


    def _handle_response(self, message):
        if message == self.ffi.NULL:
            print("Received NULL message.")
            return None

        # Identify the type of the message
        message_type = self.ffi.typeof(message).cname

        # If message is a pointer to CommandResponse, dereference it
        if message_type == "CommandResponse *":
            message = message[0]  # Dereference the pointer
            message_type = self.ffi.typeof(message).cname
        # Check if message is now a CommandResponse
        if message_type == "CommandResponse":
            msg = message
            if msg.response_type == 0:  # Null
                return None
            elif msg.response_type == 1:  # Int
                return msg.int_value
            elif msg.response_type == 2:  # Float
                return msg.float_value
            elif msg.response_type == 3:  # Bool
                return bool(msg.bool_value)
            elif msg.response_type == 4:  # String
                try:
                    string_value = self.ffi.buffer(msg.string_value, msg.string_value_len)[:]
                    return string_value
                except Exception as e:
                    print(f"Error decoding string value: {e}")
            elif msg.response_type == 5:  # Array
                array = []
                for i in range(msg.array_value_len):
                    element = self.ffi.cast("struct CommandResponse*", msg.array_value + i)
                    array.append(self._handle_response(element))
                return array
            elif msg.response_type == 6:  # Map
                map_dict = {}
                for i in range(msg.array_value_len):
                    key = self.ffi.cast("struct CommandResponse*", msg.map_key + i)
                    value = self.ffi.cast("struct CommandResponse*", msg.map_value + i)
                    map_dict[self._handle_response(key)] = self._handle_response(value)
                return map_dict
            elif msg.response_type == 7:  # Sets
                result_set = set()
                sets_array = self.ffi.cast(f"struct CommandResponse[{msg.sets_value_len}]", msg.sets_value)
                for i in range(msg.sets_value_len):
                    element = sets_array[i]  # Already a struct
                    result_set.add(self._handle_response(element))
                return result_set
            else:
                print(f"Unhandled response type = {msg.response_type}")
                return None
        else:
            print(f"Unexpected message type: {message_type}")
            return None    

    def _to_c_strings(self, args):
        """Convert Python arguments to C-compatible pointers and lengths."""
        c_strings = []
        string_lengths = []
        buffers = []  # Keep a reference to prevent premature garbage collection

        for arg in args:
            if isinstance(arg, str):
                # Convert string to UTF-8 bytes
                arg_bytes = arg.encode('utf-8')
            elif isinstance(arg, (int, float)):
                # Convert numeric values to strings and then to bytes
                arg_bytes = str(arg).encode('utf-8')
            else:
                raise ValueError(f"Unsupported argument type: {type(arg)}")

            # Use ffi.from_buffer for zero-copy conversion
            buffers.append(arg_bytes)  # Keep the byte buffer alive
            c_strings.append(self.ffi.cast("size_t", self.ffi.from_buffer(arg_bytes)))
            string_lengths.append(len(arg_bytes))

        # Return C-compatible arrays and keep buffers alive
        return (
            self.ffi.new("size_t[]", c_strings),
            self.ffi.new("unsigned long[]", string_lengths),
            buffers,  # Ensure buffers stay alive
        )
    
    async def _execute_command(
        self,
        request_type: RequestType.ValueType,
        args: List[TEncodable],
        route: Optional[Route] = None,
    ) -> TResult:
        client_adapter_ptr = self.core_client
        if client_adapter_ptr == self.ffi.NULL:
            raise ValueError("Invalid client pointer.")
        callback_idx = self._get_callback_index()
        response_future = self._get_future(callback_idx)
        # Convert the arguments to C-compatible pointers
        c_args, c_lengths, buffers = self._to_c_strings(args)
        route_bytes= b""
        c_route_bytes = self.ffi.from_buffer(route_bytes)
        # Call the command function
        self.lib.command(
            client_adapter_ptr,  # Client pointer
            callback_idx,  # Example channel (adjust as needed)
            request_type,  # Request type (e.g., GET or SET)
            len(args),  # Number of arguments
            c_args,  # Array of argument pointers
            c_lengths,  # Array of argument lengths
            c_route_bytes,
            len(route_bytes)
        )
        await response_future
        return response_future.result()
