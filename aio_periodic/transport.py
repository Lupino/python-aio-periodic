import asyncio
import logging

logger = logging.getLogger('aio_periodic.transport')


async def open_connection(entrypoint):
    logger.info("open connection: " + entrypoint)
    if entrypoint.startswith('unix://'):
        reader, writer = await asyncio.open_unix_connection(
            entrypoint.split('://')[1])
    else:
        host_port = entrypoint.split('://')[1].split(':')
        reader, writer = await asyncio.open_connection(host_port[0],
                                                       host_port[1])

    return reader, writer
