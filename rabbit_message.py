"""
This module contains a messaging system implementation that uses RabbitMQ
"""
import asyncio
import json

import aio_pika

from message_system_interface import MessageSystemInterface, MessageSystemFactory


class RabbitFactory(MessageSystemFactory):
    """
    This class creates a RabbitMQ connection and returns a
    MessageSystemInterface object.
    """
    def __init__(self, config: str) -> None:
        self.connection = aio_pika.Connection(config)
        self.channel = aio_pika.Channel(self.connection)
        self.exchange = aio_pika.Exchange(
                                          self.channel,
                                          "ActorExchange",
                                          "direct")
        self.broadcast = aio_pika.Exchange(
                                           self.channel,
                                           "BroadcastExchange",
                                           "fanout")
        

    def create(self) -> MessageSystemInterface:
        """
        Creates a new RabbitMQ connection and returns a MessageSystemInterface
        object.
        """
        return RabbitMessage(self.connection, self.channel, self.exchange, self.broadcast)


class RabbitMessage(MessageSystemInterface):
    """
    This class implements a basic messaging system that uses RabbitMQ.
    """
    def __init__(self, connection, channel, exchange, broadcast):
        self.connection = connection
        self.channel = channel
        self.exchange = exchange
        self.broadcast_ex = broadcast
        self.queue = None
        self.address = None

    async def ensure_connected(self):
        """
        Provides the asynchronous part of the connection procedure. This would
        normally have been done automatically by the standard functions in the
        aio-pika library, but we need to do it manually since we insist on
        creating the connection, channel and exchange objects in synchronous
        contexts.
        """

        if not self.connection.connected.is_set():
            # The library author advises against this, but it's necessary if we
            # wish to provide connection details from a synchronous context.
            # Normally, the aio_pika.connect function would both instantiate
            # the connection object synchronously and await the connect
            # function on the resulting connection object.
            await self.connection.connect()

            # Once the connection object has been connected, we have to attach
            # the channel to the connection object asynchronously. Normally
            # this would be done already by the asynchronous Connection.channel
            # method, but we have to do it manually.
            #await self.channel.initialize()

    async def mailman(self, actor):
        """
        Ensures all queues and exchanges are in order, and then creates a
        consumer for the actor's queue.
        """
        await self.ensure_connected()
        await self.exchange.declare()  # Make sure the exchange is declared
        self.address = actor.address
        self.queue = await self.channel.declare_queue(self.address)
        await self.queue.bind(self.exchange, self.address)

        async def consumer(message):
            """
            Decodes the message from JSON to a dictionary and deposits it
            in the actor's queue. If the queue is full or can't be decoded,
            the message is Nack'ed. If the message is successfully sent, it
            is Ack'ed.
            """
            try:
                msg = json.loads(message.body.decode())
                actor.queue.put_nowait(msg)
                await message.ack()
            except asyncio.QueueFull:
                await message.nack()
            except json.JSONDecodeError:
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
        msg = json.dumps(message).encode()
        msg_object = aio_pika.Message(body=msg)
        await self.exchange.publish(msg_object, routing_key=address)

    async def register_shard(self, address):
        """
        Registers the shard.
        """
        await self.broadcast_ex.declare()
        await self.queue.bind(self.broadcast_ex, self.address)
    
    async def broadcast(self, message):
        """
        Broadcasts a message to all shards.
        """
        msg = json.dumps(message).encode()
        msg_object = aio_pika.Message(body=msg)
        await self.broadcast_ex.publish(msg_object)