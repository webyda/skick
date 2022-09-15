"""
In this module, a messaging system that combines the SimpleMessage and
RabbitMessage messaging systems is defined. They are composed in such a way
that if a message is to be sent within a shard SimpleMessage will be used,
but if the message is to be sent between shards, RabbitMessage will be used.
"""

from typing import Any
import asyncio

from .message_system_interface import MessageSystemInterface, MessageSystemFactory
from .simple_message import SimpleFactory
from .rabbit_message import RabbitFactory

class CombinedMessage(MessageSystemInterface):
    """
    Merely contains a RabbitMessage and a SimpleMessage
    """
    def __init__(self, RabbitMessage, SimpleMessage) -> None:
        
        self.RabbitMessage = RabbitMessage
        self.SimpleMessage = SimpleMessage

    async def mailman(self, actor: "Actor") -> asyncio.Task:
        """
        Create queues for SimpleMessage and RabbitMessage
        """
        await self.SimpleMessage.mailman(actor)
        await self.RabbitMessage.mailman(actor)

    async def send(self, address: str, message: dict) -> None:
        """
        Sends a message using SimpleMessage if the address is in the
        SimpleMessage queue. If not it sends the message using
        RabbitMessage.send().
        """
        if address in self.SimpleMessage.queues:
            await self.SimpleMessage.send(address, message)
        else:
            await self.RabbitMessage.send(address, message)
    
    async def register_shard(self, address):
        """
        This method is concerned with inter-shard communication, so it falls
        back to the RabbitMessage version of the method.
        """
        await self.RabbitMessage.register_shard(address)
    
    async def broadcast(self, message: dict) -> None:
        """
        This method is used in shard synchronisation, so it falls back to the
        RabbitMesage version of the method.
        """
        await self.RabbitMessage.broadcast(message)

class CombinedFactory(MessageSystemFactory):
    """
    This class creates a SimpleFactory and a RabbitFactory and returns a
    MessageSystemInterface object.
    """

    def __init__(self, config: Any) -> None:
        
        self.SimpleFactory = SimpleFactory(config)
        self.RabbitFactory = RabbitFactory(config)

    def create(self) -> CombinedMessage:
        """
        Creates a new SimpleMessage and a new RabbitMessage
        """
        RabbitMessage = self.RabbitFactory.create()
        SimpleMessage = self.SimpleFactory.create()
        
        return CombinedMessage(RabbitMessage, SimpleMessage)
