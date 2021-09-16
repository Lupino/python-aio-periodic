from aio_periodic import open_connection, Job, ClientCluster
import asyncio
from time import time


async def main(loop):
    client = ClientCluster(['tcp://:5000', 'tcp://:5001'], loop=loop)
    await client.connect(open_connection)

    job = Job(func='echo', name='test_echo')
    job2 = Job(func='echo',
               name='test_echo2',
               sched_at=int(time()) + 10000,
               timeout=10)
    job3 = Job(func='echo', name='test_echo3', sched_at=int(time()) + 1)
    job4 = Job(func='echo2', name='test_echo3', sched_at=int(time()) + 1)
    job5 = Job(func='echo_later', name='test_echo_later')
    print(await client.submit_job(job=job))
    print(await client.run_job(job=job))

    print(await client.submit_job(job=job2))
    print(await client.remove_job('echo', 'test_echo2'))

    print(await client.run_job(job=job3))

    print(await client.submit_job(job=job4))
    print(await client.status())
    print(await client.drop_func('echo2'))
    print(await client.status())
    print(await client.run_job(job=job5))
    try:
        print(await client.run_job(job=job4))
    except Exception as e:
        print(e)

    for i in range(0, 100):
        job6 = Job(func='test_lock', name='test_lock_' + str(i))
        await client.submit_job(job=job6)


loop = asyncio.get_event_loop_policy().get_event_loop()

loop.run_until_complete(main(loop))
