"""
This module contains a messaging system implementation that uses RabbitMQ
"""
import asyncio
import orjson

import aio_pika

from .message_system_interface import MessageSystemInterface, MessageSystemFactory


class RabbitFactory(MessageSystemFactory):
    """
    This class creates a RabbitMQ connection and returns a
    MessageSystemInterface object.
    """

    def __init__(self, config: str, loop=None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self.future = self.loop.create_future()
        self.connection_task = self.loop.create_task(self.connect(config))

    async def connect(self, config):
        """
        Sets up the connection to the RabbitMQ server and stores the
        connections, channels and exchanges in a future allowing
        RabbitMessage objects to postpone extracting references to them until
        the task is finished at some point in the future.
        """
        connection = await aio_pika.connect_robust(config, loop=self.loop)
        channel = await connection.channel()
        exchange = await channel.declare_exchange("ActorExchange", "direct")
        broadcast = await channel.declare_exchange("BroadcastExchange", "fanout")

        self.future.set_result((connection, channel, exchange, broadcast))

    def create(self) -> MessageSystemInterface:
        """
        Creates a new RabbitMQ connection and returns a MessageSystemInterface
        object.
        """
        return RabbitMessage(self.future)


class RabbitMessage(MessageSystemInterface):
    """
    This class implements a basic messaging system that uses RabbitMQ.
    """

    def __init__(self, future):
        self.future = future

        # The following will be set by the mailman when the future is done.
        self.connection = None
        self.channel = None
        self.exchange = None
        self.broadcast_ex = None

        self.queue = None
        self.address = None

    async def mailman(self, actor):
        """
        Ensures all queues and exchanges are in order, and then creates a
        consumer for the actor's queue.
        """
        (
            self.connection,
            self.channel,
            self.exchange,
            self.broadcast_ex,
        ) = await self.future

        self.queue = await self.channel.declare_queue(actor.name)
        await self.queue.bind(self.exchange, actor.name)

        async def consumer(message):
            """
            Decodes the message from JSON to a dictionary and deposits it
            in the actor's queue. If the queue is full or can't be decoded,
            the message is Nack'ed. If the message is successfully sent, it
            is Ack'ed.
            """
            try:
                msg = orjson.loads(message.body.decode())
                await actor.queue.put(msg)
                await message.ack()
            except asyncio.QueueFull:
                await message.nack()
            except orjson.JSONDecodeError:
                await message.nack()

        await self.queue.consume(consumer, consumer_tag=actor.name)

        async def cleanup():
            await self.queue.cancel(actor.name)
            await self.queue.delete()

        return cleanup

    async def send(self, address, message):
        """
        Sends a json encodable (presumably a dictionary) message to the
        specified address over RabbitMQ.
        """
        await self.future  # might cause performance concerns?
        msg = orjson.dumps(message)
        msg_object = aio_pika.Message(body=msg)
        await self.exchange.publish(msg_object, routing_key=address, mandatory=False)

    async def register_shard(self, address):
        """
        Registers a shard, allowing it to receive broadcasts.
        """
        await self.future
        await self.broadcast_ex.declare()
        await self.queue.bind(self.broadcast_ex, address)

    async def unregister_shard(self, address):
        """
        Unregisters the shard, preventing it from receiving broadcasts.
        """
        await self.future
        await self.queue.unbind(self.broadcast_ex, address)

    async def broadcast(self, message):
        """
        Broadcasts a message to all registered shards.
        """
        await self.future
        msg = orjson.dumps(message).encode()
        msg_object = aio_pika.Message(body=msg)
        await self.broadcast_ex.publish(msg_object, routing_key="")
