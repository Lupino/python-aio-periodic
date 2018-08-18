import asyncio
import struct
import os


NOOP           = b'\x00'
# for job
GRAB_JOB       = b'\x01'
SCHED_LATER    = b'\x02'
JOB_DONE       = b'\x03'
JOB_FAIL       = b'\x04'
JOB_ASSIGN     = b'\x05'
NO_JOB         = b'\x06'
# for func
CAN_DO         = b'\x07'
BROADCAST      = b'\x15'
CANT_DO        = b'\x08'
# for test
PING           = b'\x09'
PONG           = b'\x0A'
# other
SLEEP          = b'\x0B'
UNKNOWN        = b'\x0C'
# client command
SUBMIT_JOB     = b'\x0D'
STATUS         = b'\x0E'
DROP_FUNC      = b'\x0F'
REMOVE_JOB     = b'\x11'

RUN_JOB        = b'\x19'
WORK_DATA      = b'\x1A'

SUCCESS        = b'\x10'

MAGIC_REQUEST  = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# client type

TYPE_CLIENT = b'\x01'
TYPE_WORKER = b'\x02'

def to_bytes(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return bytes(s, 'utf-8')
    else:
        return bytes(str(s), 'utf-8')

def to_str(s):
    if isinstance(s, bytes):
        return str(s, 'utf-8')
    elif isinstance(s, str):
        return s
    else:
        return str(s)

def to_int(s):
    return int(s)


class ConnectionError(Exception):
    pass


class BaseAgent(object):
    def __init__(self, writer, msgid, loop=None):
        self._writer = writer
        self.msgid = msgid
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
        buf = bytes(self._buffer)
        self._buffer.clear()
        return buf

    @asyncio.coroutine
    def send(self, payload):
        if isinstance(payload, list):
            payload = b''.join([to_bytes(p) for p in payload])
        elif isinstance(payload, str):
            payload = bytes(payload, 'utf-8')
        if self.msgid:
            payload = msgid + payload
        self._writer.write(MAGIC_REQUEST + encode_int32(payload))
        yield from self._writer.drain()

    def _make_waiter(self):
        waiter = self._waiter
        assert waiter is None or waiter.cancelled()
        waiter = asyncio.Future(loop=self._loop)
        self._waiter = waiter
        return waiter


def open_connection(entrypoint):
    if entrypoint.startswith('unix://'):
        reader, writer = yield from asyncio.open_unix_connection(
            entrypoint.split('://')[1])
    else:
        host_port = entrypoint.split('://')[1].split(':')
        reader, writer = yield from asyncio.open_connection(host_port[0],
                                                            host_port[1])

    return reader, writer


class BaseClient(object):
    def __init__(self, clientType, loop=None):
        self.connected = False
        self._reader = None
        self._writer = None
        self.agents = dict()
        self.clientType = clientType
        if not loop:
            loop = asyncio.get_event_loop()
        self.loop = loop

    def _connect(self):
        self._reader, self._writer = yield from open_connection(
            self._entryPoint)

        agent = BaseAgent(self._writer, None, self.loop)
        yield from agent.send(self.clientType)
        asyncio.Task(self.loop_agent())
        self.connected = True
        return True

    def add_server(self, entryPoint):
        self._entryPoint = entryPoint

    @property
    def agent(self):
        msgid = msgid.msgid1()
        agent = BaseAgent(self._writer, msgid, self.loop)
        self.agents[msgid] = agent
        return agent

    def loop_agent(self):
        while True:
            magic = yield from self._reader.read(4)
            if not magic:
                break
            if magic != MAGIC_RESPONSE:
                raise Exception('Magic not match.')
            header = yield from self._reader.read(4)
            length = decode_int32(header)
            payload = yield from self._reader.read(length)
            msgid = payload[0:4]
            agent = self.agents[msgid]
            agent.feed_data(payload[4:])

    def connect(self):
        try:
            ret = yield from self.ping()
            if ret:
                self.connected = True
                return True
        except Exception:
            pass

        print('Try to reconnecting %s'%(self._entryPoint))
        connected = yield from self._connect()
        return connected

    def ping(self):
        agent = self.agent
        yield from agent.send(PING)
        payload = yield from agent.recive()
        self.agents.pop(agent.msgid)
        if payload == PONG:
            return True
        return False

    def remove_agent(self, agent):
        self.agents.pop(agent.msgid, None)

    def close(self):
        if self._writer:
            self._writer.close()

def encode_str8(data = b''):
    return encode_int8(len(data)) + data

def encode_str32(data = b''):
    return encode_int32(len(data)) + data

def encode_int8(n = 0):
    return struct.pack('>B', n)

def encode_int16(n = 0):
    return struct.pack('>H', n)

def encode_int32(n = 0):
    return struct.pack('>I', n)

def encode_int64(n = 0):
    return struct.pack('>Q', n)

def decode_int8(n):
    return struct.unpack('>B', n)

def decode_int16(n):
    return struct.unpack('>H', n)

def decode_int32(n):
    return struct.unpack('>I', n)

def decode_int64(n):
    return struct.unpack('>Q', n)

def encode_job(job):
    return ''.join([
        encode_str8(job['func']),
        encode_str8(job['name']),
        encode_str32(job.get('workload', b'')),
        encode_int64(job.get('sched_at', 0)),
        encode_int32(job.get('count', 0))
    ])

def decode_job(payload):
    job = {}

    h = decode_int8(payload[0:1])
    job['func'] = payload[1:h + 1]

    payload = payload[h + 1:]

    h = decode_int8(payload[0:1])
    job['name'] = payload[1:h + 1]

    payload = payload[h + 1:]

    h = decode_int32(payload[0:4])
    job['workload'] = payload[4:h+4]
    payload = payload[h+4:]

    job['sched_at'] = decode_int64(payload[0:8])

    job['count'] = decode_int16(payload[8:12])
    return job
