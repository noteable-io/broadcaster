import aioredis
import asyncio
import logging
import typing
from .base import BroadcastBackend
from .._base import Event

logger = logging.getLogger("broadcaster.redis")


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        self.conn_url = url

        self._pub_conn: typing.Optional[aioredis.Redis] = None
        self._sub_conn: typing.Optional[aioredis.Redis] = None

        self._msg_queue: typing.Optional[asyncio.Queue] = None
        self._tasks: typing.Dict[str, asyncio.Task] = {}

    async def connect(self) -> None:
        if self._pub_conn or self._sub_conn or self._msg_queue:
            logger.warning("connections are already setup but connect called again; not doing anything")
            return

        self._pub_conn = await aioredis.create_redis(self.conn_url)
        self._sub_conn = await aioredis.create_redis(self.conn_url)
        self._msg_queue = asyncio.Queue()  # must be created here, to get proper event loop

    async def disconnect(self) -> None:
        if not (self._pub_conn and self._sub_conn):
            logger.warning("connections are not setup, invalid call to disconnect")
            return

        self._pub_conn.close()
        self._sub_conn.close()

        self._pub_conn = None
        self._sub_conn = None
        self._msg_queue = None

    async def subscribe(self, channel: str) -> None:
        if not self._sub_conn:
            logger.error(f"not connected, cannot subscribe to channel {channel!r}")
            return

        channels = await self._sub_conn.subscribe(channel)
        self._tasks[channel] = asyncio.create_task(self._reader(channels[0]), name=f"{channel} reader")

    async def unsubscribe(self, channel: str) -> None:
        if not self._sub_conn:
            logger.error(f"not connected, cannot unsubscribe from channel {channel!r}")
            return

        await self._sub_conn.unsubscribe(channel)

        if channel not in self._tasks:
            logger.warning(f"{channel} is not in task list, unable to wait for it to terminate")
            return

        await self._tasks[channel]

    async def publish(self, channel: str, message: typing.Any) -> None:
        if not self._pub_conn:
            logger.error(f"not connected, cannot publish to channel {channel!r}")
            return

        await self._pub_conn.publish_json(channel, message)

    async def next_published(self) -> Event:
        if not self._msg_queue:
            raise RuntimeError("unable to get next_published event, RedisBackend is not connected")

        return await self._msg_queue.get()

    async def _reader(self, channel: aioredis.Channel) -> None:
        while await channel.wait_message():
            msg = await channel.get_json()

            if not self._msg_queue:
                logger.error(f"unable to put new message from {channel.name.decode('utf8')} into queue, not connected")
                continue

            await self._msg_queue.put(Event(channel=channel.name.decode("utf8"), message=msg))
