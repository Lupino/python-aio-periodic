import asyncio
import logging
from asyncio import StreamReader, StreamWriter

logger = logging.getLogger('aio_periodic.transport')


async def open_connection(
        entrypoint: str) -> tuple[StreamReader, StreamWriter]:
    logger.info("open connection: " + entrypoint)
    if entrypoint.startswith('unix://'):
        return await asyncio.open_unix_connection(entrypoint.split('://')[1])
    else:
        host_port = entrypoint.split('://')[1].split(':')
        return await asyncio.open_connection(host_port[0], host_port[1])
