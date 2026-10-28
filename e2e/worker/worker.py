"""taskiq worker for the e2e suite (see internal/web/e2e_test.go).

`taskiq worker worker:broker` runs the worker; `python worker.py` kicks
20 `ok` tasks then 2 `slow` ones and exits.
"""

import asyncio
import os

from taskiq.middlewares.taskiq_admin_middleware import TaskiqAdminMiddleware
from taskiq_redis import RedisStreamBroker

broker = RedisStreamBroker(url=os.environ["REDIS_URL"], queue_name="taskiq").with_middlewares(
    TaskiqAdminMiddleware(url=os.environ["ADMIN_URL"], api_token="e2e", taskiq_broker_name="e2e"),
)


# Explicit names: kicked from __main__, the default names would be
# "__main__:ok", unknown to the worker.
@broker.task(task_name="worker:ok")
async def ok() -> str:
    return "ok"


@broker.task(task_name="worker:slow")
async def slow() -> str:
    await asyncio.sleep(30)
    return "slow"


async def main() -> None:
    await broker.startup()
    for _ in range(20):
        await ok.kiq()
    for _ in range(2):
        await slow.kiq()
    # Waits for the middleware's pending "queued" reports.
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
