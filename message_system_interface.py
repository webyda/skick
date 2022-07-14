"""
Provides an interface for messaging systems like RabbitMQ through which
they must be able to communicate with actors.
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Any
#from actor import Actor

class MessageSystemInterface(ABC):
    """ Interface for message systems """

    @abstractmethod
    async def mailman(self, actor: "Actor") -> asyncio.Task:
        """
        This method provides a consumer mechanism. It should return a task
        which receives messages asynchronously but in turn, and subsequently
        places them in the queue referenced by queue.
        """
        pass

    @abstractmethod
    async def send(self, address: str, message: dict) -> None:
        """
        This method must provide a means to send a message to the actor at the
        address specified.
        """
        pass
    
class MessageSystemFactory(ABC):
    """
    This abstract class provides an interface for setting up stable
    configurations for messaging systems. For example, in RabbitMQ, it might
    carry a connection object while generating MessageSystemInterface objects
    which are given separate channels. This allows us to abstract away the
    boilerplate code for setting up individual actors' messaging systems.
    """
    @abstractmethod
    def __init__(self, config: Any) -> None:
        """ We require an __init__ method with a specific interface """
        pass
    

    @abstractmethod
    def create(self) -> MessageSystemInterface:
        """
        This method must be implemented by concrete subclasses. It must return
        a MessageSystemInterface object.
        """
        pass