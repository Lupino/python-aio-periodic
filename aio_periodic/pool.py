import asyncio
from time import time

class Pool(object):
    def __init__(self, init, size, timeout = 0):
        self.init = init
        self._entryPoint = None
        self._size = size
        self._sem = asyncio.Semaphore(size)
        self.locker = asyncio.Lock()

        self._timeout = timeout
        self._deadline = 0
        self.client = None

    def _get(self):
        if self._deadline > 0 and self.client and self._deadline < time():
            self.client.close()
            self.client = None
        if self.client:
            try:
                yield from self.client.ping()
            except:
                self.client.close()
                self.client = None

        if self.client:
            return self.client
        client = yield from self.init()
        if self._timeout > 0:
            self._deadline = time() + self._timeout

        self.client = client
        return client

    def get(self):
        with (yield from self.locker):
            client = yield from self._get()
            yield from self._sem.acquire()
            return client


    def release(self):
        return self._sem.release()
