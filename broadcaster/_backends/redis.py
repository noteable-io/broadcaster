import aioredis
import asyncio
import typing
from .base import BroadcastBackend
from .._base import Event


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        self.conn_url = url

        self._pub_conn: typing.Optional[aioredis.Redis] = None
        self._sub_conn: typing.Optional[aioredis.Redis] = None
        self._msg_queue = asyncio.Queue()

        self._tasks_lock = asyncio.Lock()
        self._tasks = []

    async def connect(self) -> None:
        self._pub_conn = await aioredis.create_redis(self.conn_url)
        self._sub_conn = await aioredis.create_redis(self.conn_url)

    async def disconnect(self) -> None:
        self._pub_conn.close()
        self._sub_conn.close()

        self._pub_conn = None
        self._sub_conn = None

    async def subscribe(self, channel: str) -> None:
        channels = await self._sub_conn.subscribe(channel)

        async with self._tasks_lock:
            self._tasks.append(asyncio.ensure_future(self.reader(channels[0])))

    async def unsubscribe(self, channel: str) -> None:
        await self._sub_conn.unsubscribe(channel)

        async with self._tasks_lock:
            for task in self._tasks:
                await task

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._pub_conn.publish_json(channel, message)

    async def next_published(self) -> Event:
        return await self._msg_queue.get()

    async def reader(self, channel: aioredis.Channel):
        while await channel.wait_message():
            msg = await channel.get_json()
            await self._msg_queue.put(Event(channel=channel.name.decode("utf8"), message=msg))
