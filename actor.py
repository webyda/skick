"""
Contains a base Actor class, being the fundamental object of actor models.
"""
import asyncio
from typing import Callable, Awaitable

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
                 queue_size: int = 100,
                 loop: asyncio.AbstractEventLoop = None,
                 shard: str = None) -> None:
        self.name = name
        self.queue = asyncio.Queue(queue_size)
        self.loop = loop or asyncio.get_event_loop()
        self._actions = {}
        self._conversation_prototypes = {}
        self._daemons = {}
        self._on_start = None
        self._on_stop = None
        
        self._daemon_tasks = {}
        self._message_task = None
        self._mailman_cleanup = None
        self._message_system = message_system
        self._shard = shard  # lets us record the name of the shard
        
        self._conversations = {} # A list of active "conversations"

    async def _process_reply(self, message: dict) -> None:
        """
        This method interfaces with the conversation object. It ensures that
        messages are sent to the correct conversation, that responses are
        expedited and that conversations end when they are supposed to.
        """
        if message["action"] == "end_conversation":
            await self._end_conversation(message)
        elif message["action"] == "receive_reply":
            conversation = self._conversations[message["cid"]]
            reply = await conversation(message)
            if reply:
                (destination, reply) = reply
                await self.send(destination, reply)
                if reply["action"] == "end_conversation":
                    await self._end_conversation(reply)
            else:
                pass
                
    async def _start_conversation(self, generator):
        """
        This method is invoked when an action indicates that it is in fact
        a conversation type action. It then returns a generator, the first
        element of which is a Conversation object, which contains the necessary
        information to manage the conversation. We simply store it in the
        _conversations dictionary after having performed some initial setup.
        """
        conversation = await generator.asend(None)
        greeting = await conversation.attach(generator, self.name)
        self._conversations[conversation.cid] = conversation
        if greeting:
            """
            In this case, we have a *call* and ret is the message we need to
            send to the other party to get the conversation started
            """
            await self.send(conversation.partner, greeting)

        else:
            """
            In this case, we are dealing with a respond action, where the
            actor is expecting to deliver the first reply. In this case, we
            simply need to reiterate as the message will come later.
            """
            response = await generator.__anext__()
            await self.send(conversation.partner, {"action": "receive_reply",
                                                   "cid": conversation.cid,
                                                   "sender": self.name,
                                                   "message": response})

    async def _end_conversation(self, message: dict) -> None:
        """
        This method is invoked when an action indicates that it is an 
        end_conversation action. It then removes the relevant conversation
        from the _conversations dictionary after having run the optional
        cleanup function.
        """
        if message["cid"] in self._conversations:
            await self._conversations[message["cid"]].close()
            del self._conversations[message["cid"]]
        else:
            pass

    async def _process_messages(self) -> None:
        """
        Consumes incoming messages from the internal queue and activates the
        appropriate handler. There a few different things can happen:

        1. It is an ordinary action, in which case the handler is awaited.
        2. It is a reply in an active conversation, in which case the handling
           is deferred to the _process_reply method.
        3. The action returned something True, in which case it is a
           generator, and we need to handle it as a convesation start. 
        """
        while True:
            message = await self.queue.get()
            if message["action"] in self._actions:
                await self._actions[message["action"]](message)

            elif message["action"] in self._conversation_prototypes:
                generator = self._conversation_prototypes[message["action"]](message)
                if generator:
                    await self._start_conversation(generator)
                else:
                    pass
            elif message["action"] in ["receive_reply", "end_conversation"]:
                await self._process_reply(message)
            else:
                pass

    def action(self, name: str) -> (
        Callable[
            [
                Callable[[dict], Awaitable[None]],
            ],
            Callable[[dict], Awaitable[None]]
            ]):
        """ Adds a regular action to the actor. """
        def decorator(func):
            self._actions[name] = func
            return func
        return decorator

    def conversation(self, name):
        """ Adds a generator to the appropriate dictionary """
        def decorator(gen):
            self._conversation_prototypes[name] = gen
            return gen
        return decorator
    
    def daemon(self, name):
        """
        Registers a daemon that will be launched as a task when the actor
        starts, and which will be cancelled when the actor stops or is
        replaced.
        """
        def decorator(func):
            self._daemons[name] = func
            return func
        return decorator

    def on_start(self, func):
        """
        Registers a method which will be run asynchronously when the actor is
        started.
        """

        self._on_start = func
        return func

    def on_stop(self, func):
        """
        Registers a method which will be run asynchronously when the actor
        is stopped
        """
        self._on_stop = func
        return func
        
    async def replace(self,
                      factory: Callable[["Actor", dict], Awaitable[None]],
                      message: dict) -> None:
        """
        Replaces the current actor's behaviors with another. Also replaces
        the state encapsulating closure. It does this by running the factory
        function of the new actor after having cleared the _actions dict.
        """
        self._actions.clear()
        
        for task in self._daemon_tasks.values():
            task.cancel()    
        self._daemons.clear()
        self._daemon_tasks.clear()
        if self._on_stop:
            await self._on_stop()
        factory(self, message)
        if self._on_start:
            await self._on_start()
        for name, func in self._daemons.items():
            self._daemon_tasks[name] = self.loop.create_task(func())



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

        if self._on_start:
            await self._on_start()

        self._daemon_tasks = {key: self.loop.create_task(item())
                              for key, item in self._daemons.items()}
        for task in self._daemon_tasks.values():
            task.add_done_callback(self._error_callback)

        self._message_task = self.loop.create_task(self._process_messages())
        self._message_task.add_done_callback(self._error_callback)

    async def stop(self) -> None:
        """ Kills the actor and cleans up """
        if self._message_task:
            self._message_task.cancel()

        for task in self._daemon_tasks.values():
            task.cancel()

        if self._on_stop:
            await self._on_stop()

        if self._mailman_cleanup:
            await self._mailman_cleanup()

    def _error_callback(self, task):
        if not task.cancelled():
            if exception := task.exception():
                print(exception)
                raise exception
            else:
                pass
        else:
            pass
