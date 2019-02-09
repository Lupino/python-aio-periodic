from aio_periodic import open_connection, Client, Job
import asyncio
from time import time

async def main(loop):
    client = Client(loop)
    reader, writer = await open_connection('unix:///tmp/periodic.sock')
    await client.connect(reader, writer)

    job = Job(func='echo', name='test_echo')
    job2 = Job(func='echo', name='test_echo2', sched_at=int(time()) + 10000, timeout=10)
    job3 = Job(func='echo', name='test_echo3', sched_at=int(time()) + 1)
    job4 = Job(func='echo2', name='test_echo3', sched_at=int(time()) + 1)
    job5 = Job(func='echo_later', name='test_echo_later')
    print(await client.ping())
    print(await client.submit_job(job))
    print(await client.run_job(job))

    print(await client.submit_job(job2))
    print(await client.remove_job('echo', 'test_echo2'))

    print(await client.run_job(job3))

    print(await client.submit_job(job4))
    print(await client.status())
    print(await client.drop_func('echo2'))
    print(await client.status())
    print(await client.run_job(job5))



loop = asyncio.get_event_loop()

loop.run_until_complete(main(loop))
