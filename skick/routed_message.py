"""
Contains a message system that uses routing hints from the addressing module
to route messages to the right shard. This way, we can use one queue per shard
and avoid the overhead of creating a new queue for each actor. This is necesary
because without it, starting new actors is too time consuming. On the flip side,
RabbitMQ uses one thread per queue, so this may hinder it from scaling properly.
"""

import asyncio
import orjson

import aio_pika

from .message_system_interface import MessageSystemInterface, MessageSystemFactory
from .simple_message import SimpleFactory


class RoutedMessage(MessageSystemInterface):
    """
    Merely contains a RabbitMessage and a SimpleMessage
    """

    def __init__(self, factory, simple_interface) -> None:
        self.factory = factory
        self.simple_interface = simple_interface

    async def mailman(self, actor):
        """Sets up a queue for the local interface."""
        return await self.simple_interface.mailman(actor)

    async def send(self, address, message):
        """
        Selects a method for sending the message by looking at the address.
        If the address is locally available, it will use the SimpleMessage
        system. Otherwise, it will use the RabbitMQ system to send the
        message to the appropriate shard, and *then* have it be forwarded to
        the recipient using that shard's SimpleMessage system. This way only
        one amqp queue is required per shard.
        """
        if address in self.simple_interface.queues:
            return await self.simple_interface.send(address, message)

        else:
            shard = address[: len(address) // 2]  # This is faster than using split
            return await self.factory.rabbit_send(shard, [address, message])

    async def register_shard(self, address):
        """
        This is where the local SimpleMessage system is connected to the rest
        of the cluster. The address is the address of the invoking shard, and
        upon invocation, a queue will be declared that can relay messages into
        the local messaging system. Simultaneously, a task is created to
        consume messages from the queue and transfer them. This way, we only
        need to create one queue per shard and do not need to interact with
        the rabbitmq server at all when we spawn new child actors.
        """
        await self.factory.future
        shard = address[: len(address) // 2]
        self.factory.shard = address
        self.factory.queue = await self.factory.out_channel.declare_queue(
            shard, auto_delete=True
        )
        await self.factory.queue.bind(self.factory.in_exchange, routing_key=shard)
        await self.factory.queue.bind(self.factory.in_broadcast, routing_key=shard)

        await self.factory.queue.consume(self.factory.transfer, consumer_tag=shard)

    async def unregister_shard(self, address):
        """Deletes the queue"""
        await self.factory.queue.delete(if_unused=False, if_empty=False)

    async def broadcast(self, message):
        """
        Broadcasts a message to all shards
        """
        await self.factory.future
        msg = orjson.dumps(["", message])
        msg_object = aio_pika.Message(body=msg)
        await self.factory.out_broadcast.publish(msg_object, routing_key="")


class RoutedFactory(MessageSystemFactory):
    """
    A factory for creating RoutedMessage objects. Unlike in the CombinedMessage
    class, the SimpleMessge system is always used to deliver the message to the
    actor's mailbox. This is because there is only one queue per shard. This
    queue is managed in the factory and is used to receive messages from other
    shards. When a message is received in this manner, it is forwarded to the
    appropriate actors in the shard.
    """

    def __init__(self, config: str, loop=None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self.future = self.loop.create_future()
        self.connection_task = self.loop.create_task(self.connect(config))

        self.in_connection = None
        self.out_connection = None
        self.channel = None
        self.exchange = None
        self.broadcast = None

        self.queue = None
        self.shard = None
        self.simple_factory = SimpleFactory(config, loop=loop)

    async def connect(self, config):
        """
        Sets up the connection to the RabbitMQ server and stores the
        connections, channels and exchanges in a future allowing
        RabbitMessage objects to postpone extracting references to them until
        the task is finished at some point in the future.
        """
        self.in_connection = await aio_pika.connect(config, loop=self.loop)
        self.out_connection = await aio_pika.connect(config, loop=self.loop)
        self.in_channel = await self.in_connection.channel(publisher_confirms=False)
        self.out_channel = await self.out_connection.channel(publisher_confirms=False)
        self.in_exchange = await self.in_channel.declare_exchange(
            "ActorExchange", "direct"
        )
        self.out_exchange = await self.out_channel.declare_exchange(
            "ActorExchange", "direct"
        )
        self.out_broadcast = await self.out_channel.declare_exchange(
            "BroadcastExchange", "fanout"
        )
        self.in_broadcast = await self.in_channel.declare_exchange(
            "BroadcastExchange", "fanout"
        )

        self.future.set_result(
            (
                self.in_connection,
                self.out_connection,
                self.in_channel,
                self.out_channel,
                self.out_exchange,
                self.out_broadcast,
                self.in_exchange,
                self.in_broadcast,
            )
        )

    async def rabbit_send(self, shard, message):
        """
        Used by RoutedMessage to send messages between shards using RabbitMQ.
        """
        if self.out_exchange:
            msg = orjson.dumps(message)
            msg_object = aio_pika.Message(body=msg)
            return await self.out_exchange.publish(
                msg_object, routing_key=shard, mandatory=False
            )
        else:
            return False

    async def transfer(self, message):
        """
        Used as a consumer in RoutedMessage to transfer messages from
        RabbitMQ to the shard local messaging system.
        """
        try:

            [address, msg] = orjson.loads(message.body)
            success = await self.simple_factory.send(address or self.shard, msg)
            if success:
                await message.ack()
            else:
                await message.nack()
        except orjson.JSONDecodeError:
            pass
            await message.nack()
        except TypeError:
            pass
            await message.nack()

    def create(self) -> MessageSystemInterface:
        """
        Creates a new RabbitMQ connection and returns a MessageSystemInterface
        object.
        """
        return RoutedMessage(self, self.simple_factory.create())
