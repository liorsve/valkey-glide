from glide.commands.sync_commands.core import CoreCommands
from typing import List, Optional, cast
from glide.constants import TResult, TEncodable
from glide.protobuf.command_request_pb2 import RequestType
from glide.routes import Route
from glide.constants import TClusterResponse

class ClusterCommands(CoreCommands):
    def custom_command(
        self, command_args: List[TEncodable], route: Optional[Route] = None
    ) -> TClusterResponse[TResult]:
        """
        Executes a single command, without checking inputs.
        See the [Valkey GLIDE Wiki](https://github.com/valkey-io/valkey-glide/wiki/General-Concepts#custom-command)
        for details on the restrictions and limitations of the custom command API.

            For example - Return a list of all pub/sub clients from all nodes::

                connection.customCommand(["CLIENT", "LIST","TYPE", "PUBSUB"], AllNodes())

        Args:
            command_args (List[TEncodable]): List of the command's arguments, where each argument is either a string or bytes.
            Every part of the command, including the command name and subcommands, should be added as a separate value in args.
            route (Optional[Route]): The command will be routed automatically based on the passed command's default request
                policy, unless `route` is provided, in which
            case the client will route the command to the nodes defined by `route`. Defaults to None.

        Returns:
            TClusterResponse[TResult]: The returning value depends on the executed command and the route.
        """
        return cast(
            TClusterResponse[TResult],
            self._execute_command(RequestType.CustomCommand, command_args, route),
        )
