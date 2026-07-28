"""Define callback contracts shared by periodic task registration and execution."""

from .job import Job
from typing import Awaitable, Callable, Tuple, Union
from .rsp import ResponseTypes
from .types.utils import Encodable

# A response controls the job lifecycle; a raw payload completes the job with
# that value as its encoded result.
TaskResult = Union[ResponseTypes, Encodable]
# A synchronous task callback that receives its scheduled job.
SyncTaskFunc = Callable[[Job], TaskResult]
# An asynchronous task callback that resolves to a task result.
ASyncTaskFunc = Callable[[Job], Awaitable[TaskResult]]
# Either supported implementation style for a periodic task callback.
TaskFunc = Union[ASyncTaskFunc, SyncTaskFunc]
# A callback that derives a distributed lock key and its concurrency limit.
LockerFunc = Callable[[Job], Tuple[str, int]]
