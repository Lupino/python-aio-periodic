import asyncio
from time import time
from typing import Any, Callable, Coroutine
from .types.base_client import BaseClient


class Pool(object):
    init: Callable[[], Coroutine[Any, Any, BaseClient]]
    _sem: asyncio.Semaphore
    locker: asyncio.Lock
    _timeout: int
    _deadline: int
    client: BaseClient | None

    def __init__(self,
                 init: Callable[[], Coroutine[Any, Any, BaseClient]],
                 size: int,
                 timeout: int = 0) -> None:
        self.init = init
        self._sem = asyncio.Semaphore(size)
        self.locker = asyncio.Lock()

        self._timeout = timeout
        self._deadline = 0
        self.client = None

    async def _get(self) -> BaseClient:
        if self._deadline > 0 and self.client and self._deadline < time():
            self.client.close()
            self.client = None
        if self.client:
            try:
                await self.client.ping()
            except Exception:
                self.client.close()
                self.client = None

        if self.client:
            return self.client
        client = await self.init()
        if self._timeout > 0:
            self._deadline = int(time()) + self._timeout

        self.client = client
        return client

    async def get(self) -> BaseClient:
        async with self.locker:
            client = await self._get()
            await self._sem.acquire()
            return client

    def release(self) -> None:
        return self._sem.release()
