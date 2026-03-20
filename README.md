The periodic task system client for Python 3 based on `asyncio`.

Install
-------

```bash
pip3 install aio_periodic
```

Quick Start
-----------

```python
from aio_periodic import Client, Transport, Job
import asyncio


async def main() -> None:
    client = Client()
    await client.connect(Transport("tcp://127.0.0.1:5000"))

    result = await client.run_job(job=Job(func="echo", name="demo"))
    print(result)

    # Explicit close does not auto-reconnect.
    client.close()


asyncio.run(main())
```

Connection Lifecycle
--------------------

- `client.close()` closes the connection and does not reconnect.
- `client.close(reconnect=True)` closes and starts reconnecting in background.
