import asyncio

class Pool(object):
    def __init__(self, init, size):
        self.init = init
        self._entryPoint = None
        self._size = size
        self._sem = asyncio.Semaphore(size)

        self._clients = []
        self._locked = []
        self._last_id = 0


    def get(self):
        yield from self._sem.acquire()
        client = None
        dead_clients = []
        for cl in self._clients:
            if cl.client_id not in self._locked:
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
                if cl.client_id in self._locked:
                    self._locked.remove(cl.client_id)
                self._clients.remove(cl)

        if client:
            self._locked.append(client.client_id)
            return client

        client = yield from self.init()
        self._last_id += 1
        client.client_id = self._last_id
        self._locked.append(client.client_id)

        self._clients.append(client)
        return client


    def release(self, client):
        if client.client_id in self._locked:
            self._locked.remove(client.client_id)
        return self._sem.release()
