import asyncio
import logging
from asyncio import StreamReader, StreamWriter

logger = logging.getLogger('aio_periodic.transport')


class Transport(object):
    entrypoint: str

    def __init__(self, entrypoint: str) -> None:
        self.entrypoint = entrypoint

    async def get(self) -> tuple[StreamReader, StreamWriter]:
        logger.info("open connection: " + self.entrypoint)
        entrypoint = self.entrypoint.split('://')
        if entrypoint[0] == 'unix':
            return await asyncio.open_unix_connection(entrypoint[1])
        else:
            host_port = entrypoint[1].split(':')
            return await asyncio.open_connection(host_port[0], host_port[1])
