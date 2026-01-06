import asyncio
from time import time
from typing import Any, Callable, Coroutine, Optional
from contextlib import asynccontextmanager
from .types.base_client import BaseClient

# Type alias for the initialization function
InitFunc = Callable[[], Coroutine[Any, Any, BaseClient]]


class Pool(object):
    """
    Manages a shared BaseClient connection with a concurrency limit.
    Ensures the client is recreated if it times out or disconnects.
    """
    init: InitFunc
    _sem: asyncio.Semaphore
    locker: asyncio.Lock
    _timeout: int
    _deadline: int
    client: Optional[BaseClient]

    def __init__(self, init: InitFunc, size: int, timeout: int = 0) -> None:
        self.init = init
        # Semaphore limits the number of concurrent operations on the client
        self._sem = asyncio.Semaphore(size)
        self.locker = asyncio.Lock()

        self._timeout = timeout
        self._deadline = 0
        self.client = None

    async def _get(self) -> BaseClient:
        """Internal method to validate or recreate the connection."""
        now = time()

        # Check if the current connection has exceeded its lifespan
        if self._deadline > 0 and self.client and self._deadline < now:
            self.client.close()
            self.client = None

        # Perform a health check (Ping)
        if self.client:
            try:
                await self.client.ping()
            except Exception:
                self.client.close()
                self.client = None

        if self.client:
            return self.client

        # Initialize a new connection
        client = await self.init()
        if self._timeout > 0:
            self._deadline = int(time()) + self._timeout

        self.client = client
        return client

    async def get(self) -> BaseClient:
        """
        Acquire a client instance.
        WARNING: You must call pool.release() manually after use.
        Recommended: Use `async with pool.connection() as client:` instead.
        """
        # Acquire semaphore first to avoid holding the creation lock
        # while waiting for a concurrency slot.
        await self._sem.acquire()

        async with self.locker:
            try:
                return await self._get()
            except Exception:
                # If connection fails, release the slot immediately
                self._sem.release()
                raise

    def release(self) -> None:
        """Release the concurrency slot."""
        self._sem.release()

    @asynccontextmanager
    async def connection(self) -> Any:
        """
        Context manager for safe client acquisition and release.
        Usage:
            async with pool.connection() as client:
                await client.do_work()
        """
        client = await self.get()
        try:
            yield client
        finally:
            self.release()
