"""
In this module, a simple messaging system for single process use is defined.
it merely keeps a dictionary of message queues and puts messages in the correct
one.
"""

from typing import Any
from orjson import loads, dumps
import json

from .message_system_interface import MessageSystemInterface, MessageSystemFactory


class SimpleMessage(MessageSystemInterface):
    """
    Merely owns a dictionary of queues it uses to transfer messages directly
    to the message queue of the recipient.
    """
    def __init__(self,queues: dict, send) -> None:
        self.queues = queues
        self.shard = None
        self.send_func=send
        
    async def mailman(self, actor):
        """
        For this type of queue, mailman simply attaches the queue to the
        dictionary of queues.
        """
        self.queues[actor.name] = actor.queue
        
    async def register_shard(self, address):
        """ Registers the shard """
        self.shard = address
    
    async def send(self, address, message):
        if self.send_func:
            return await self.send_func(address, message)
        else:
            return False
        
    async def broadcast(self, message: dict) -> None:
        """ Sends a message to the shard """
        if self.shard:
            await self.send(self.shard, message)
        else:
            pass
        


class SimpleFactory(MessageSystemFactory):
    """
    Uses a dictionary to transfer objects between actors.
    """
    def __init__(self, config: Any, loop=None) -> None:
        """ The user may leave a config object. We will simply ignore it. """
        self.queues = {}

    def create(self) -> SimpleMessage:
        """
        Creates a new SimpleMessage object with our self.queues dictionary
        attached. The Name parameter will simply be ignored.
        """
        return SimpleMessage(self.queues, self.send)

    async def send(self, address: str, message: dict) -> None:
        """
        Simply selects the queue from the dictionary and appends the message.
        We convert the message to json and then back again, because if we don't
        we may experience bugs where the programmer relied on the message being
        serialized and deserialized. A concrete example from testing is this:
        an actor contains a list of strings. The actor sends the list to its
        subscribers. The subscribers then append items to their local version
        of the list after the actor sends updates. In this case, unless we
        perform a deep copy, SimpleMessage will send a *reference* to the
        original list, and the same item will appear many times because
        the updates are appended once per subscriber. This will not happen
        in messaging systems that transmit JSON to a server. In order to
        replicate the exact behavior of such servers, we perform the same
        serialization-deserialization procedure.
        """
        
        if address in self.queues:
            try:
                self.queues[address].put_nowait(loads(dumps(message)))
            except asyncio.QueueFull:
                return False
            except json.JSONDecodeError:
                return False
            else:
                return True
        else:
            return False