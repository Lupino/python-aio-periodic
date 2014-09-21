import asyncio
from time import time

class Pool(object):
    def __init__(self, init, size, timeout = 0):
        self.init = init
        self._entryPoint = None
        self._size = size
        self._sem = asyncio.Semaphore(size)

        self._timeout = timeout
        self._deadline = 0
        self.client = None

    def get(self):
        yield from self._sem.acquire()
        if self._deadline > 0 and self.client and self._deadline < time():
            self.client.close()
            self.client = None
        if self.client:
            return self.client
        client = yield from self.init()
        if self._timeout > 0:
            self._deadline = time() + self._timeout

        self.client = client
        return client

    def release(self, client):
        return self._sem.release()

    def remove(self, client):
        return self._sem.release()
