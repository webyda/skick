"""
In this module, a simple messaging system for single process use is defined.
it merely keeps a dictionary of message queues and puts messages in the correct
one.
"""

import asyncio
from typing import Any

from message_system_interface import MessageSystemInterface, MessageSystemFactory
        
        
class SimpleMessage(MessageSystemInterface):
    """
    Merely owns a dictionary of queues it uses to transfer messages directly
    to the message queue of the recipient.
    """
    def __init__(self, name, queues: dict) -> None:
        self.queues = queues
        
    async def mailman(self, actor):
        """
        For this type of queue, mailman simply attaches the queue to the
        dictionary of queues.
        """
        self.queues[actor.name] = actor.queue
        
    async def send(self, address: str, message: dict) -> None:
        """
        Simply selects the queue from the dictionary and appends the message
        """
        if address in self.queues:
            await self.queues[address].put(message)
        else:
            pass


class SimpleFactory(MessageSystemFactory):
    """
    Uses a dictionary to transfer objects between actors.
    """
    def __init__(self, config: Any) -> None:
        """ The user may leave a config object. We will simply ignore it. """
        self.queues = {}
    def create(self) -> SimpleMessage:
        """
        Creates a new SimpleMessage object with our self.queues dictionary
        attached. The Name parameter will simply be ignored.
        """
        return SimpleMessage("name", self.queues)
        