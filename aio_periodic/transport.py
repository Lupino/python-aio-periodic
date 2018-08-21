import asyncio

def open_connection(entrypoint):
    if entrypoint.startswith('unix://'):
        reader, writer = yield from asyncio.open_unix_connection(
            entrypoint.split('://')[1])
    else:
        host_port = entrypoint.split('://')[1].split(':')
        reader, writer = yield from asyncio.open_connection(host_port[0],
                                                            host_port[1])

    return reader, writer
