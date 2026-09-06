import asyncio
import datetime as dt
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika

from .core.config import settings

Event = dict[str, Any]
Handler = Callable[[Event], Awaitable[None]]
Binding = tuple[str, str, list[str], Handler]


async def publish(
    exchange: str, event_type: str, data: Event, correlation_id: str
) -> None:
    connection = await aio_pika.connect_robust(settings.amqp_url)
    async with connection:
        channel = await connection.channel()
        target = await channel.declare_exchange(
            exchange, aio_pika.ExchangeType.TOPIC, durable=True
        )
        body = {
            "id": str(uuid.uuid4()),
            "type": event_type,
            "source": settings.service_name,
            "time": dt.datetime.now(dt.UTC).isoformat(),
            "correlation_id": correlation_id,
            "data": data,
        }
        await target.publish(
            aio_pika.Message(json.dumps(body).encode(), content_type="application/json"),
            routing_key=event_type,
        )


async def consume(bindings: list[Binding]) -> None:
    if not bindings:
        await asyncio.Future()
    connection = await aio_pika.connect_robust(settings.amqp_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=20)
    for exchange, queue_name, keys, handler in bindings:
        source = await channel.declare_exchange(
            exchange, aio_pika.ExchangeType.TOPIC, durable=True
        )
        queue = await channel.declare_queue(queue_name, durable=True)
        for key in keys:
            await queue.bind(source, routing_key=key)

        def make_callback(
            selected_handler: Handler,
        ) -> Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]:
            async def callback(message: aio_pika.abc.AbstractIncomingMessage) -> None:
                async with message.process(requeue=True):
                    await selected_handler(json.loads(message.body))

            return callback

        await queue.consume(make_callback(handler))
    await asyncio.Future()
