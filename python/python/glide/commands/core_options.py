
# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional, Type, Union, get_args

from glide.commands.command_args import Limit, OrderBy
from glide.constants import TEncodable


class ConditionalChange(Enum):
    """
    A condition to the `SET`, `ZADD` and `GEOADD` commands.
    - ONLY_IF_EXISTS - Only update key / elements that already exist. Equivalent to `XX` in the Valkey API.
    - ONLY_IF_DOES_NOT_EXIST - Only set key / add elements that does not already exist. Equivalent to `NX` in the Valkey API.
    """

    ONLY_IF_EXISTS = "XX"
    ONLY_IF_DOES_NOT_EXIST = "NX"
    
@dataclass
class OnlyIfEqual:
    """
    Change condition to the `SET` command,
    For additional conditonal options see ConditionalChange

    - comparison_value - value to compare to the current value of a key.

    If comparison_value is equal to the key, it will overwrite the value of key to the new provided value
    Equivalent to the IFEQ comparison-value in the Valkey API
    """

    comparison_value: TEncodable

class ExpiryType(Enum):
    """SET option: The type of the expiry.
    - SEC - Set the specified expire time, in seconds. Equivalent to `EX` in the Valkey API.
    - MILLSEC - Set the specified expire time, in milliseconds. Equivalent to `PX` in the Valkey API.
    - UNIX_SEC - Set the specified Unix time at which the key will expire, in seconds. Equivalent to `EXAT` in the Valkey API.
    - UNIX_MILLSEC - Set the specified Unix time at which the key will expire, in milliseconds. Equivalent to `PXAT` in the
        Valkey API.
    - KEEP_TTL - Retain the time to live associated with the key. Equivalent to `KEEPTTL` in the Valkey API.
    """

    SEC = 0, Union[int, timedelta]  # Equivalent to `EX` in the Valkey API
    MILLSEC = 1, Union[int, timedelta]  # Equivalent to `PX` in the Valkey API
    UNIX_SEC = 2, Union[int, datetime]  # Equivalent to `EXAT` in the Valkey API
    UNIX_MILLSEC = 3, Union[int, datetime]  # Equivalent to `PXAT` in the Valkey API
    KEEP_TTL = 4, Type[None]  # Equivalent to `KEEPTTL` in the Valkey API


class ExpiryTypeGetEx(Enum):
    """GetEx option: The type of the expiry.
    - EX - Set the specified expire time, in seconds. Equivalent to `EX` in the Valkey API.
    - PX - Set the specified expire time, in milliseconds. Equivalent to `PX` in the Valkey API.
    - UNIX_SEC - Set the specified Unix time at which the key will expire, in seconds. Equivalent to `EXAT` in the Valkey API.
    - UNIX_MILLSEC - Set the specified Unix time at which the key will expire, in milliseconds. Equivalent to `PXAT` in the
        Valkey API.
    - PERSIST - Remove the time to live associated with the key. Equivalent to `PERSIST` in the Valkey API.
    """

    SEC = 0, Union[int, timedelta]  # Equivalent to `EX` in the Valkey API
    MILLSEC = 1, Union[int, timedelta]  # Equivalent to `PX` in the Valkey API
    UNIX_SEC = 2, Union[int, datetime]  # Equivalent to `EXAT` in the Valkey API
    UNIX_MILLSEC = 3, Union[int, datetime]  # Equivalent to `PXAT` in the Valkey API
    PERSIST = 4, Type[None]  # Equivalent to `PERSIST` in the Valkey API


class InfoSection(Enum):
    """
    INFO option: a specific section of information:

    -SERVER: General information about the server
    -CLIENTS: Client connections section
    -MEMORY: Memory consumption related information
    -PERSISTENCE: RDB and AOF related information
    -STATS: General statistics
    -REPLICATION: Master/replica replication information
    -CPU: CPU consumption statistics
    -COMMANDSTATS: Valkey command statistics
    -LATENCYSTATS: Valkey command latency percentile distribution statistics
    -SENTINEL: Valkey Sentinel section (only applicable to Sentinel instances)
    -CLUSTER: Valkey Cluster section
    -MODULES: Modules section
    -KEYSPACE: Database related statistics
    -ERRORSTATS: Valkey error statistics
    -ALL: Return all sections (excluding module generated ones)
    -DEFAULT: Return only the default set of sections
    -EVERYTHING: Includes all and modules
    When no parameter is provided, the default option is assumed.
    """

    SERVER = "server"
    CLIENTS = "clients"
    MEMORY = "memory"
    PERSISTENCE = "persistence"
    STATS = "stats"
    REPLICATION = "replication"
    CPU = "cpu"
    COMMAND_STATS = "commandstats"
    LATENCY_STATS = "latencystats"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"
    MODULES = "modules"
    KEYSPACE = "keyspace"
    ERROR_STATS = "errorstats"
    ALL = "all"
    DEFAULT = "default"
    EVERYTHING = "everything"


class ExpireOptions(Enum):
    """
    EXPIRE option: options for setting key expiry.

    - HasNoExpiry: Set expiry only when the key has no expiry (Equivalent to "NX" in Valkey).
    - HasExistingExpiry: Set expiry only when the key has an existing expiry (Equivalent to "XX" in Valkey).
    - NewExpiryGreaterThanCurrent: Set expiry only when the new expiry is greater than the current one (Equivalent
        to "GT" in Valkey).
    - NewExpiryLessThanCurrent: Set expiry only when the new expiry is less than the current one (Equivalent to "LT" in Valkey).
    """

    HasNoExpiry = "NX"
    HasExistingExpiry = "XX"
    NewExpiryGreaterThanCurrent = "GT"
    NewExpiryLessThanCurrent = "LT"


class UpdateOptions(Enum):
    """
    Options for updating elements of a sorted set key.

    - LESS_THAN: Only update existing elements if the new score is less than the current score.
    - GREATER_THAN: Only update existing elements if the new score is greater than the current score.
    """

    LESS_THAN = "LT"
    GREATER_THAN = "GT"


class ExpirySet:
    """SET option: Represents the expiry type and value to be executed with "SET" command."""

    def __init__(
        self,
        expiry_type: ExpiryType,
        value: Optional[Union[int, datetime, timedelta]],
    ) -> None:
        """
        Args:
            - expiry_type (ExpiryType): The expiry type.
            - value (Optional[Union[int, datetime, timedelta]]): The value of the expiration type. The type of expiration
                determines the type of expiration value:
                - SEC: Union[int, timedelta]
                - MILLSEC: Union[int, timedelta]
                - UNIX_SEC: Union[int, datetime]
                - UNIX_MILLSEC: Union[int, datetime]
                - KEEP_TTL: Type[None]
        """
        self.set_expiry_type_and_value(expiry_type, value)

    def set_expiry_type_and_value(
        self, expiry_type: ExpiryType, value: Optional[Union[int, datetime, timedelta]]
    ):
        if not isinstance(value, get_args(expiry_type.value[1])):
            raise ValueError(
                f"The value of {expiry_type} should be of type {expiry_type.value[1]}"
            )
        self.expiry_type = expiry_type
        if self.expiry_type == ExpiryType.SEC:
            self.cmd_arg = "EX"
            if isinstance(value, timedelta):
                value = int(value.total_seconds())
        elif self.expiry_type == ExpiryType.MILLSEC:
            self.cmd_arg = "PX"
            if isinstance(value, timedelta):
                value = int(value.total_seconds() * 1000)
        elif self.expiry_type == ExpiryType.UNIX_SEC:
            self.cmd_arg = "EXAT"
            if isinstance(value, datetime):
                value = int(value.timestamp())
        elif self.expiry_type == ExpiryType.UNIX_MILLSEC:
            self.cmd_arg = "PXAT"
            if isinstance(value, datetime):
                value = int(value.timestamp() * 1000)
        elif self.expiry_type == ExpiryType.KEEP_TTL:
            self.cmd_arg = "KEEPTTL"
        self.value = str(value) if value else None

    def get_cmd_args(self) -> List[str]:
        return [self.cmd_arg] if self.value is None else [self.cmd_arg, self.value]


class ExpiryGetEx:
    """GetEx option: Represents the expiry type and value to be executed with "GetEx" command."""

    def __init__(
        self,
        expiry_type: ExpiryTypeGetEx,
        value: Optional[Union[int, datetime, timedelta]],
    ) -> None:
        """
        Args:
            - expiry_type (ExpiryType): The expiry type.
            - value (Optional[Union[int, datetime, timedelta]]): The value of the expiration type. The type of expiration
                determines the type of expiration value:
                - SEC: Union[int, timedelta]
                - MILLSEC: Union[int, timedelta]
                - UNIX_SEC: Union[int, datetime]
                - UNIX_MILLSEC: Union[int, datetime]
                - PERSIST: Type[None]
        """
        self.set_expiry_type_and_value(expiry_type, value)

    def set_expiry_type_and_value(
        self,
        expiry_type: ExpiryTypeGetEx,
        value: Optional[Union[int, datetime, timedelta]],
    ):
        if not isinstance(value, get_args(expiry_type.value[1])):
            raise ValueError(
                f"The value of {expiry_type} should be of type {expiry_type.value[1]}"
            )
        self.expiry_type = expiry_type
        if self.expiry_type == ExpiryTypeGetEx.SEC:
            self.cmd_arg = "EX"
            if isinstance(value, timedelta):
                value = int(value.total_seconds())
        elif self.expiry_type == ExpiryTypeGetEx.MILLSEC:
            self.cmd_arg = "PX"
            if isinstance(value, timedelta):
                value = int(value.total_seconds() * 1000)
        elif self.expiry_type == ExpiryTypeGetEx.UNIX_SEC:
            self.cmd_arg = "EXAT"
            if isinstance(value, datetime):
                value = int(value.timestamp())
        elif self.expiry_type == ExpiryTypeGetEx.UNIX_MILLSEC:
            self.cmd_arg = "PXAT"
            if isinstance(value, datetime):
                value = int(value.timestamp() * 1000)
        elif self.expiry_type == ExpiryTypeGetEx.PERSIST:
            self.cmd_arg = "PERSIST"
        self.value = str(value) if value else None

    def get_cmd_args(self) -> List[str]:
        return [self.cmd_arg] if self.value is None else [self.cmd_arg, self.value]


class InsertPosition(Enum):
    BEFORE = "BEFORE"
    AFTER = "AFTER"


class FlushMode(Enum):
    """
    Defines flushing mode for:

    `FLUSHALL` command and `FUNCTION FLUSH` command.

    See https://valkey.io/commands/flushall/ and https://valkey.io/commands/function-flush/ for details

    SYNC was introduced in version 6.2.0.
    """

    ASYNC = "ASYNC"
    SYNC = "SYNC"


class FunctionRestorePolicy(Enum):
    """
    Options for the FUNCTION RESTORE command.

    - APPEND: Appends the restored libraries to the existing libraries and aborts on collision. This is the
        default policy.
    - FLUSH: Deletes all existing libraries before restoring the payload.
    - REPLACE: Appends the restored libraries to the existing libraries, replacing any existing ones in case
        of name collisions. Note that this policy doesn't prevent function name collisions, only libraries.
    """

    APPEND = "APPEND"
    FLUSH = "FLUSH"
    REPLACE = "REPLACE"


def _build_sort_args(
    key: TEncodable,
    by_pattern: Optional[TEncodable] = None,
    limit: Optional[Limit] = None,
    get_patterns: Optional[List[TEncodable]] = None,
    order: Optional[OrderBy] = None,
    alpha: Optional[bool] = None,
    store: Optional[TEncodable] = None,
) -> List[TEncodable]:
    args = [key]

    if by_pattern:
        args.extend(["BY", by_pattern])

    if limit:
        args.extend(["LIMIT", str(limit.offset), str(limit.count)])

    if get_patterns:
        for pattern in get_patterns:
            args.extend(["GET", pattern])

    if order:
        args.append(order.value)

    if alpha:
        args.append("ALPHA")

    if store:
        args.extend(["STORE", store])

    return args
