from .job import Job
from typing import Any, Callable, Coroutine, Union, Tuple

# Type Aliases for cleaner signatures
SyncTaskFunc = Callable[[Job], Any]
ASyncTaskFunc = Callable[[Job], Coroutine[Any, Any, Any]]
TaskFunc = Union[ASyncTaskFunc, SyncTaskFunc]
LockerFunc = Callable[[Job], Tuple[str, int]]
