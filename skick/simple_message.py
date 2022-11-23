"""
In this module, a simple messaging system for single process use is defined.
it merely keeps a dictionary of message queues and puts messages in the correct
one.
"""
import asyncio
from typing import Any
from copy import deepcopy

from .message_system_interface import MessageSystemInterface, MessageSystemFactory


class SimpleMessage(MessageSystemInterface):
    """
    Merely owns a dictionary of queues it uses to transfer messages directly
    to the message queue of the recipient.
    """

    def __init__(self, queues: dict, send) -> None:
        self.queues = queues
        self.shard = None
        self.send_func = send

    async def mailman(self, actor):
        """
        For this type of queue, mailman simply attaches the queue to the
        dictionary of queues.
        """
        self.queues[actor.name] = actor.queue

        async def cleanup():
            if actor.name in self.queues:
                del self.queues[actor.name]

        return cleanup

    async def register_shard(self, address):
        """Registers the shard"""
        self.shard = address

    async def unregister_shard(self, address):
        """Unregisters the shard. Nothing remarkable here."""
        self.shard = None

    async def send(self, address, message):
        """Sends a message to some mailbox."""
        if self.send_func:
            return await self.send_func(address, message)
        else:
            return False

    async def broadcast(self, message: dict) -> None:
        """Sends a message to the shard"""
        if self.shard:
            await self.send(self.shard, message)
        else:
            pass


class SimpleFactory(MessageSystemFactory):
    """
    Uses a dictionary to transfer objects between actors.
    """

    def __init__(self, config: Any, loop=None) -> None:
        """The user may leave a config object. We will simply ignore it."""
        self.queues = {}

    def create(self) -> SimpleMessage:
        """
        Creates a new SimpleMessage object with our self.queues dictionary
        attached.
        """
        return SimpleMessage(self.queues, self.send)

    async def send(self, address: str, message: dict) -> None:
        """
        Simply selects the queue from the dictionary and appends the message.
        We make a deep copy of the dictionary to prevent bugs arising from
        the mutable nature of dictionaries.

        Previously, we serialized and deserialized the message in order to
        simulate what a remote message system would do. While this technically
        helps the programmer by providing consistent behavior betwen the
        message systems, but it is a little bit wasteful, especially since some
        messages may be serialized and unserialized a number of times before finally
        reaching their destination. Removing one such pass could provide some improvement.
        """

        if address in self.queues:
            try:
                self.queues[address].put_nowait(deepcopy(message))
            except asyncio.QueueFull:
                return False
            else:
                return True
        else:
            return False
