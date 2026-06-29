import asyncio
import logging
import os

from aio_periodic import Transport, Worker, RSATransport
from aio_periodic.job import Job


async def echo(job: Job) -> None:
    print(job.name)
    await job.done(job.name)


async def echo_later(job: Job) -> None:
    print(job.name, job.payload.count)
    if job.payload.count > 3:
        await job.done(job.name)
    else:
        await job.sched_later(1, 1)


async def test_lock(job: Job) -> None:

    async def do_lock() -> None:
        await asyncio.sleep(1)
        await echo(job)

    await job.with_lock('test', 2, do_lock)


def current_mode() -> str:
    return os.getenv('PERIODIC_MODE') or os.getenv('PERIODIC_RSA_MODE') or 'Normal'


async def main() -> None:
    worker = Worker(
        [],
        client_name=os.getenv('PERIODIC_WORKER_NAME'),
        client_token=os.getenv('PERIODIC_WORKER_TOKEN'),
    )
    tp = Transport('tcp://127.0.0.1:5000')
    mode = current_mode()
    if mode != 'Normal':
        rsa_tp = RSATransport(
            tp,
            os.getenv('PERIODIC_CLIENT_PRIVATE_KEY', 'client_private.pem'),
            os.getenv('PERIODIC_SERVER_PUBLIC_KEY', 'server_public.pem'),
        )
        await worker.connect(rsa_tp)
    else:
        await worker.connect(tp)

    await worker.add_func('echo', echo)
    await worker.add_func('echo_later', echo_later)
    await worker.add_func('test_lock', test_lock)
    await worker.work_until_shutdown(10)


if __name__ == '__main__':
    f0 = '%(levelname)s - %(message)s'
    formatter = '[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} ' + f0
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.run(main())
