import asyncio
from .agent import Agent
from .utils import decode_int32

class BaseClient(object):
    def __init__(self, clientType, loop=None):
        self.connected = False
        self._reader = None
        self._writer = None
        self._clientType = clientType
        self.agents = dict()
        if not loop:
            loop = asyncio.get_event_loop()
        self.loop = loop

    def connect(self, reader, writer, clientType):
        self._writer = writer
        self._reader = reader
        agent = Agent(self._writer, None, self.loop)
        yield from agent.send(slef._clientType)
        asyncio.Task(self.loop_agent())
        self.connected = True
        return True

    @property
    def agent(self):
        msgid = msgid.msgid1()
        agent = Agent(self._writer, msgid, self.loop)
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
