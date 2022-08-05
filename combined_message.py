"""
In this module, a messaging system that combine Simple and Rabbit messaging systems is defined.
If the message is to be sent within servers Simple will be used.
If the message is to be sent between servers Rabbit will be used.
"""

from typing import Any
import asyncio

from message_system_interface import MessageSystemInterface, MessageSystemFactory

from simple_message import SimpleFactory

from rabbit_message import RabbitFactory

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
        Sends a message using SimpleMessage.send() 
        if the address is in the SimpleMessage queue.
        If not it sends a message using RabbitMessage.send().
        """
        if address in self.SimpleMessage.queues:
            await self.SimpleMessage.send(address, message)
        else:
            await self.RabbitMessage.send(address, message)

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
