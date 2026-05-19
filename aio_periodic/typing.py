from .job import Job
from typing import Awaitable, Callable, Tuple, Union
from .rsp import ResponseTypes
from .types.utils import Encodable

# Type Aliases for cleaner signatures
TaskResult = Union[ResponseTypes, Encodable]
SyncTaskFunc = Callable[[Job], TaskResult]
ASyncTaskFunc = Callable[[Job], Awaitable[TaskResult]]
TaskFunc = Union[ASyncTaskFunc, SyncTaskFunc]
LockerFunc = Callable[[Job], Tuple[str, int]]
