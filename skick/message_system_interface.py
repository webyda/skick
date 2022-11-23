"""
Provides an interface for messaging systems like RabbitMQ through which
they must be able to communicate with actors.
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Any


class MessageSystemInterface(ABC):
    """An interface for message systems"""

    @abstractmethod
    async def mailman(self, actor: "Actor") -> asyncio.Task:
        """
        This method provides a consumer mechanism. It should return a task
        which receives messages asynchronously but in turn, and subsequently
        places them in the queue referenced by queue.
        """

    @abstractmethod
    async def send(self, address: str, message: dict) -> None:
        """
        This method must provide a means to send a message to the actor at the
        address specified.
        """

    @abstractmethod
    async def unregister_shard(self, address: str) -> None:
        """
        This method must provide a means to unregister a shard from the
        messaging system.
        """

    @abstractmethod
    async def register_shard(self, address):
        """
        This method registers the current shard with the messaging system
        so that it can receive broadcasts meant for all shards.
        """

    @abstractmethod
    async def broadcast(self, message: dict) -> None:
        """This method broadcasts a message to all shards"""


class MessageSystemFactory(ABC):
    """
    This abstract class provides an interface for setting up stable
    configurations for messaging systems. For example, in RabbitMQ, it might
    carry a connection object while generating MessageSystemInterface objects
    which are given separate channels. This allows us to abstract away the
    boilerplate code for setting up individual actors' messaging systems.
    """

    @abstractmethod
    def __init__(self, config: Any, loop=None) -> None:
        """We require an __init__ method with a specific interface"""

    @abstractmethod
    def create(self) -> MessageSystemInterface:
        """
        This method must be implemented by concrete subclasses. It must return
        a MessageSystemInterface object.
        """
