import asyncio
from .agent import Agent
from .utils import decode_int32, MAGIC_RESPONSE
import uuid
from .command import PING, PONG
from binascii import crc32

class BaseClient(object):
    def __init__(self, clientType, loop=None):
        self.connected = False
        self.connid = None
        self._reader = None
        self._writer = None
        self._clientType = clientType
        self.agents = dict()
        if not loop:
            loop = asyncio.get_event_loop()
        self.loop = loop

    async def connect(self, reader, writer):
        self._writer = writer
        self._reader = reader
        agent = Agent(self._writer, None, self.loop)
        await agent.send(self._clientType)
        self.loop.create_task(self.loop_agent())
        self.connected = True
        return True

    @property
    def agent(self):
        msgid = bytes(uuid.uuid4().hex[:4], 'utf-8')
        agent = Agent(self._writer, msgid, self.loop)
        self.agents[msgid] = agent
        return agent

    async def loop_agent(self):
        async def receive():
            magic = await self._reader.read(4)
            if not magic:
                self.close()
                raise Exception("Closed")
            if magic != MAGIC_RESPONSE:
                self.close()
                raise Exception('Magic not match.')
            header = await self._reader.read(4)
            length = decode_int32(header)
            crc = await self._reader.read(4)
            payload = await self._reader.read(length)
            if decode_int32(crc) != crc32(payload):
                raise Exception('CRC not match.')
            return payload

        self.connid = await receive()
        while True:
            payload = await receive()
            msgid = payload[0:4]
            agent = self.agents.get(msgid)
            if agent:
                agent.feed_data(payload[4:])
            else:
                print('Agent %s not found.'%msgid)

    async def ping(self):
        agent = self.agent
        await agent.send(PING)
        payload = await agent.recive()
        self.agents.pop(agent.msgid)
        if payload == PONG:
            return True
        return False

    def remove_agent(self, agent):
        self.agents.pop(agent.msgid, None)

    def close(self):
        if self._writer:
            self._writer.close()
