from cffi import FFI
from glide.protobuf.command_request_pb2 import Command, CommandRequest, RequestType
from typing import List, Union, Optional, cast
from glide.sync_commands.core import CoreCommands, InfoSection
from glide.constants import DEFAULT_READ_BYTES_SIZE, OK, TEncodable, TRequest, TResult
from glide.routes import Route
from glide.exceptions import ClosingError, RequestError
from glide.config import GlideClusterClientConfiguration
from glide.glide_client import get_request_error_class

# Enum values must match the Rust definition
class FFIClientTypeEnum:
    Async = 0
    Sync = 1
    
class GlideSync(CoreCommands):        
    def __init__(self, config):
        self._init_ffi()
        self.config = config
        conn_req = config._create_a_protobuf_conn_request(cluster_mode=type(config) == GlideClusterClientConfiguration)
        conn_req_bytes = conn_req.SerializeToString()
        client_type = self.ffi.new("ClientType*", {
            "_type": self.ffi.cast("ClientTypeEnum", FFIClientTypeEnum.Sync),
        })
        client_response_ptr = self.lib.create_client(conn_req_bytes, len(conn_req_bytes), client_type) 
        # Handle the connection response
        if client_response_ptr != self.ffi.NULL:
            client_response = self.ffi.cast("ConnectionResponse*", client_response_ptr)
            if client_response.conn_ptr != self.ffi.NULL:
                self.core_client = client_response.conn_ptr
            else:
                error_message = self.ffi.string(client_response.connection_error_message).decode('utf-8') if client_response.connection_error_message != self.ffi.NULL else "Unknown error"
                raise ClosingError(error_message)

            # Free the connection response to avoid memory leaks
            self.lib.free_connection_response(client_response_ptr)
        else:
            raise ClosingError("Failed to create client, response pointer is NULL.")

    def _init_ffi(self):
        self.ffi = FFI()

        # Define the CommandResponse struct and related types
        self.ffi.cdef("""
            typedef struct {
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
                int _type;  // Enum to differentiate between Async and Sync
                union {
                    struct {
                        void (*success_callback)(uintptr_t, const void*);
                        void (*failure_callback)(uintptr_t, const char*, int);
                    } async_client;
                };
            } ClientType;

            // Function declarations
            const ConnectionResponse* create_client(
                const uint8_t* connection_request_bytes,
                size_t connection_request_len,
                const ClientType* client_type  // Pass by pointer
            );
            void close_client(const void* client_adapter_ptr);
            void free_connection_response(ConnectionResponse* connection_response_ptr);
            char* get_response_type_string(int response_type);
            void free_response_type_string(char* response_string);
            void free_command_response(CommandResponse* command_response_ptr);
            void free_error_message(char* error_message);
            void free_command_result(CommandResult* command_result_ptr);
            CommandResult* command(const void* client_adapter_ptr, uintptr_t channel, int command_type, unsigned long arg_count, const size_t *args, const unsigned long* args_len, const unsigned char* route_bytes, size_t route_bytes_len);

        """)

        # Load the shared library (adjust the path to your compiled Rust library)
        self.lib = self.ffi.dlopen("/home/ubuntu/glide-for-redis/go/target/debug/libglide_rs.so")
        
    def _handle_response(self, message):
        if message == self.ffi.NULL:
            raise RequestError("Received NULL message.")

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
                    # TODO: Add memory cleanup in case of failures
                    raise RequestError(f"Error decoding string value: {e}")
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
                raise RequestError(f"Unhandled response type = {msg.response_type}")
        else:
            raise RequestError(f"Unexpected message type = {message_type}")
  
        

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
    
    def _handle_cmd_result(self, command_result):
        try:
            if command_result == self.ffi.NULL:
                raise ClosingError("Internal error: Received NULL as a command result")
            if command_result.command_error != self.ffi.NULL:
                # Handle the error case
                error = self.ffi.cast("CommandError*", command_result.command_error)
                error_message = self.ffi.string(error.command_error_message).decode('utf-8')
                error_class = get_request_error_class(error.command_error_type)
                # Free the error message to avoid memory leaks
                raise error_class(error_message)
            else:
                return self._handle_response(command_result.response)
                # Free the error message to avoid memory leaks
        finally:
            self.lib.free_command_result(command_result)

    def _execute_command(
        self,
        request_type: RequestType.ValueType,
        args: List[TEncodable],
        route: Optional[Route] = None,
    ) -> TResult:
        client_adapter_ptr = self.core_client
        if client_adapter_ptr == self.ffi.NULL:
            raise ValueError("Invalid client pointer.")

        # Convert the arguments to C-compatible pointers
        c_args, c_lengths, buffers = self._to_c_strings(args)
        # Call the command function
        route_bytes = b"" # TODO: add support for route 
        route_ptr = self.ffi.new("unsigned char[]", route_bytes)


        result = self.lib.command(
            client_adapter_ptr,  # Client pointer
            1,  # Example channel (adjust as needed)
            request_type,  # Request type (e.g., GET or SET)
            len(args),  # Number of arguments
            c_args,  # Array of argument pointers
            c_lengths,  # Array of argument lengths
            route_ptr,
            len(route_bytes)
        )
        return self._handle_cmd_result(result)

    # TODO: remove function once StandaloneCommands and ClusterCommands are added
    def custom_command(self, command_args: List[TEncodable]) -> TResult:
        """
        Executes a single command, without checking inputs.
        See the [Valkey GLIDE Wiki](https://github.com/valkey-io/valkey-glide/wiki/General-Concepts#custom-command)
        for details on the restrictions and limitations of the custom command API.

            @example - Return a list of all pub/sub clients:

                connection.customCommand(["CLIENT", "LIST","TYPE", "PUBSUB"])
        Args:
            command_args (List[TEncodable]): List of the command's arguments, where each argument is either a string or bytes.
            Every part of the command, including the command name and subcommands, should be added as a separate value in args.

        Returns:
            TResult: The returning value depends on the executed command.
        """
        return self._execute_command(RequestType.CustomCommand, command_args)

    def close(self):
        self.lib.close_client(self.core_client)

    # TODO: remove function once StandaloneCommands and ClusterCommands are added
    def info(
        self,
        sections: Optional[List[InfoSection]] = None,
    ) -> bytes:
        """
        Get information and statistics about the server.
        See https://valkey.io/commands/info/ for details.

        Args:
            sections (Optional[List[InfoSection]]): A list of InfoSection values specifying which sections of
            information to retrieve. When no parameter is provided, the default option is assumed.


        Returns:
            bytes: Returns bytes containing the information for the sections requested.
        """
        args: List[TEncodable] = (
            [section.value for section in sections] if sections else []
        )
        return cast(bytes, self._execute_command(RequestType.Info, args))
