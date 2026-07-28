import asyncio
from time import time
from typing import AsyncIterator, Awaitable, Callable, Optional
from contextlib import asynccontextmanager
from .types.base_client import BaseClient

# Initializers may perform asynchronous connection setup before yielding a client.
InitFunc = Callable[[], Awaitable[BaseClient]]


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
        # The client is shared, so this bounds simultaneous borrowers rather than
        # creating one connection per slot.
        self._sem = asyncio.Semaphore(size)
        # Serialize health checks and replacement so concurrent borrowers observe
        # one consistent client instance.
        self.locker = asyncio.Lock()

        self._timeout = timeout
        self._deadline = 0
        self.client = None

    async def _get(self) -> BaseClient:
        """Internal method to validate or recreate the connection."""
        now = time()

        # Expire before probing: a healthy connection must still be replaced once
        # its configured lifetime ends.
        if self._deadline > 0 and self.client and self._deadline < now:
            self.client.close()
            self.client = None

        # A failed probe invalidates the shared client for the next initializer.
        if self.client:
            try:
                await self.client.ping()
            except Exception:
                self.client.close()
                self.client = None

        if self.client:
            return self.client

        # Only set a deadline after successful initialization; failed attempts
        # must remain immediately retryable.
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
        # Do not hold the creation lock while waiting for a borrowing slot; that
        # would block clients already admitted by the semaphore from progressing.
        await self._sem.acquire()

        async with self.locker:
            try:
                return await self._get()
            except Exception:
                # A failed acquisition never transfers ownership of this slot.
                self._sem.release()
                raise

    def release(self) -> None:
        """Release the concurrency slot."""
        self._sem.release()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[BaseClient]:
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
            # The slot must be returned even when the caller raises or is
            # cancelled, otherwise later borrowers can remain blocked forever.
            self.release()
