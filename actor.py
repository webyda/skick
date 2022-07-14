"""
Contains a base Actor class, being the fundamental object of actor models.
"""
import asyncio
from typing import Callable, Awaitable, Dict

from message_system_interface import MessageSystemInterface

class Actor:
    """
    A class for actors in the Skick library.

    The actor can receive and send messages through a messaging system which
    must be provided through an abstraction called a MessageSystemInterface.
    
    After instantiation, actors are built with factory functions that supply
    them with *actions* which are async functions that process messages.
    
    Typically, these share common state through sharing a closure.
    This allows us to directly alter their state in message handlers without
    actually using the replacement mechanism, but also enables a very simple
    replacement mechanism in which the methods in question are simply
    forgotten, resulting in their closure being garbage collected.
    """
    def __init__(self, name: str,
                 message_system: MessageSystemInterface = None,
                 queue_size: int=100) -> None:
        self.name = name
        self.queue = asyncio.Queue(queue_size)
        self.loop = asyncio.get_event_loop()
        self._actions = {}

        self._message_task = None
        self._mailman_cleanup = None
        self._message_system = message_system

    async def _process_messages(self) -> None:
        """
        Consumes incoming messages from the internal queue and activates the
        appropriate handler.
        """
        while True:
            message = await self.queue.get()
            if message["action"] in self._actions:
                await self._actions[message["action"]](message)
            else:
                pass

    def action(self, name: str) -> Callable[[Callable[[Dict], Awaitable[None]]], Callable[[dict],Awaitable[None]]]:
        """ Adds a regular action to the actor. """
        def decorator(func):
            self._actions[name] = func
            return func
        return decorator

    async def replace(self,
                      factory: Callable[["Actor", dict], Awaitable[None]],
                      message: dict) -> None:
        """
        Replaces the current actor's behaviors with another. Also replaces
        the state encapsulating closure. It does this by running the factory
        function of the new actor after having cleared the _actions dict.
        """
        self._actions.clear()
        await factory(self, message)

    async def mailman(self) -> None:
        """
        This method is meant to attach some method of receiving messages to the
        actor. This relies on an abstraction of a messaging system which must
        be provided on instantiation.
        """
        self._mailman_cleanup = await self._message_system.mailman(self)
        
    async def send(self, address: str, message: dict) -> None:
        """
        This method allows the actor to send messages to other actors. It
        relies on an abstraction of a messaging system which must be injected
        on instantiation.
        """
        await self._message_system.send(address, message)

    async def run(self) -> None:
        """
        Runs the tasks associated with the actor. Since we can not use regular
        abstract base classes, we need to instead perform runtime checks to
        ensure that the actor is properly initialized.
        """

        await self.mailman()

        self._message_task = self.loop.create_task(self._process_messages())

    async def stop(self) -> None:
        """ Kills the actor and cleans up """
        if self._mailman_cleanup:
            await self._mailman_cleanup()

        if self._message_task:
            self._message_task.cancel()
