import asyncio


NOOP        = b"\x00"
# for job
GRAB_JOB    = b"\x01"
SCHED_LATER = b"\x02"
JOB_DONE    = b"\x03"
JOB_FAIL    = b"\x04"
WAIT_JOB    = b"\x05"
NO_JOB      = b"\x06"
# for func
CAN_DO      = b"\x07"
CANT_DO     = b"\x08"
# for test
PING        = b"\x09"
PONG        = b"\x0A"
# other
SLEEP       = b"\x0B"
UNKNOWN     = b"\x0C"
# client command
SUBMIT_JOB = b"\x0D"
STATUS = b"\x0E"
DROP_FUNC = b"\x0F"

SUCCESS = b"\x10"

NULL_CHAR = b"\x00\x01"


# client type

TYPE_CLIENT = b"\x01"
TYPE_WORKER = b"\x02"


def to_bytes(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return bytes(s, "utf-8")
    else:
        return bytes(s)


def parseHeader(head):
    length = head[0] << 24 | head[1] << 16 | head[2] << 8 | head[3]
    length = length & ~0x80000000

    return length


def makeHeader(data):
    header = [0, 0, 0, 0]
    length = len(data)
    header[0] = chr(length >> 24 & 0xff)
    header[1] = chr(length >> 16 & 0xff)
    header[2] = chr(length >> 8 & 0xff)
    header[3] = chr(length >> 0 & 0xff)
    return bytes(''.join(header), 'utf-8')


class ConnectionError(Exception):
    pass


class BaseAgent(object):
    def __init__(self, writer, msgId, loop=None):
        self._writer = writer
        self.msgId = msgId
        self._buffer = bytearray()
        self._loop = loop
        self._waiter = None

    def feed_data(self, data):
        self._buffer.extend(data)
        if self._waiter:
            self._waiter.set_result(True)

    @asyncio.coroutine
    def recive(self):
        waiter = self._make_waiter()
        yield from waiter
        return self._buffer

    @asyncio.coroutine
    def send(self, payload):
        if isinstance(payload, list):
            payload = [to_bytes(p) for p in payload]
            payload = NULL_CHAR.join(payload)
        elif isinstance(payload, str):
            payload = bytes(payload, 'utf-8')
        if self.msgId > 0:
            msgId = bytes(str(self.msgId), "utf-8")
            payload = msgId + NULL_CHAR + payload
        header = makeHeader(payload)
        self._writer.write(header)
        self._writer.write(payload)
        yield from self._writer.drain()

    def _make_waiter(self):
        waiter = self._waiter
        assert waiter is None or waiter.cancelled()
        waiter = asyncio.Future(loop=self._loop)
        self._waiter = waiter
        return waiter


def open_connection(entrypoint):
    if entrypoint.startswith("unix://"):
        reader, writer = yield from asyncio.open_unix_connection(entrypoint.split("://")[1])
    else:
        host_port = entrypoint.split("://")[1].split(":")
        reader, writer = yield from asyncio.open_connection(host_port[0], host_port[1])

    return reader, writer


class BaseClient(object):
    def __init__(self, clientType, loop=None):
        self.connected = False
        self._reader = None
        self._writer = None
        self._msgId = 0
        self.agents = dict()
        self.clientType = clientType
        if not loop:
            loop = asyncio.get_event_loop()
        self.loop = loop

    def _connect(self):
        self._reader, self._writer = yield from open_connection(self._entryPoint)

        self._msgId = 0
        agent = BaseAgent(self._writer, self._msgId, self.loop)
        yield from agent.send(self.clientType)
        asyncio.Task(self.loop_agent())
        self.connected = True
        return True

    def add_server(self, entryPoint):
        self._entryPoint = entryPoint

    @property
    def agent(self):
        self._msgId += 1
        agent = BaseAgent(self._writer, self._msgId, self.loop)
        self.agents[self._msgId] = agent
        return agent

    def loop_agent(self):
        while True:
            header = yield from self._reader.read(4)
            length = parseHeader(header)
            payload = yield from self._reader.read(length)
            payload = payload.split(NULL_CHAR, 1)
            msgId = int(payload[0])
            agent = self.agents[msgId]
            agent.feed_data(payload[1])

    def connect(self):
        try:
            ret = yield from self.ping()
            if ret:
                self.connected = True
                return True
        except Exception:
            pass

        print("Try to reconnecting %s"%(self._entryPoint))
        connected = yield from self._connect()
        return connected

    def ping(self):
        agent = self.agent
        yield from agent.send([PING])
        payload = yield from agent.recive()
        self.agents.pop(agent.msgId)
        if payload == PONG:
            return True
        return False

    def remove_agent(self, agent):
        self.agents.pop(agent.msgId)

    def close(self):
        if self._writer:
            self._writer.close()
