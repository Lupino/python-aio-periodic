from aio_periodic import Transport, Client, Job, RSATransport, RSAMode
import asyncio
import os
from time import time


def current_mode() -> str:
    return os.getenv('PERIODIC_MODE') or os.getenv('PERIODIC_RSA_MODE') or 'Normal'


async def main() -> None:
    client = Client()
    tp = Transport('tcp://127.0.0.1:5000')
    mode = current_mode()
    if mode != 'Normal':
        rsa_tp = RSATransport(
            tp,
            os.getenv('PERIODIC_CLIENT_PRIVATE_KEY', 'client_private.pem'),
            os.getenv('PERIODIC_SERVER_PUBLIC_KEY', 'server_public.pem'),
            mode=RSAMode[mode],
        )
        await client.connect(rsa_tp)
    else:
        await client.connect(tp)

    job = Job(func='echo', name='test_echo')
    job2 = Job(func='echo',
               name='test_echo2',
               sched_at=int(time()) + 10000,
               timeout=10)
    job3 = Job(func='echo', name='test_echo3', sched_at=int(time()) + 1)
    job4 = Job(func='echo2', name='test_echo3', sched_at=int(time()) + 1)
    job5 = Job(func='echo_later', name='test_echo_later')
    print(await client.ping())
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
        print(job6)
        await client.submit_job(job=job6)

    print('done')
    client.close()


asyncio.run(main())
