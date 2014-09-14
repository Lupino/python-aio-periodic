import asyncio

class Pool(object):
    def __init__(self, init, size):
        self.init = init
        self._entryPoint = None
        self._size = size
        self._sem = asyncio.Semaphore(size)

        self._clients = []
        self._last_id = 0


    def get(self):
        yield from self._sem.acquire()
        client = None
        dead_clients = []
        for cl in self._clients:
            if not cl.locked:
                cl.locked = True
                try:
                    if cl.ping():
                        client = cl
                        break
                    else:
                        dead_clients.append(client)
                except:
                    dead_clients.append(client)

        if dead_clients:
            for cl in dead_clients:
                self._clients.remove(cl)

        if client:
            return client

        client = yield from self.init()
        client.client_id = self._last_id
        self._last_id += 1
        client.locked = True
        self._clients.append(client)
        return client


    def release(self, client):
        client.locked = False
        return self._sem.release()
