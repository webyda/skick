"""
Contains a base Actor class, being the fundamental object of actor models.
"""
import asyncio
import traceback
from typing import Callable, Awaitable, Any
from inspect import isasyncgenfunction

from schema import Schema

from .message_system_interface import MessageSystemInterface

def schemator(schema, func):
    sch = Schema(schema)
    async def ret(message):
        if sch.is_valid(message):
            await func(message)
        else:
            pass
    return ret
    

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

        self._main_sentinel = None # The sentinel for the main task
        self._monitors = set() # A set of monitors for this actor
    
    def _default_actions(self):
        """ Adds default actions """
        self.action("monitor")(self._monitor)
        
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
            await self._reply_parser(reply)
            
    async def _reply_parser(self, reply):
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
           generator, and we need to handle it as a conversation start. 
        """
        while True:
            message = await self.queue.get()
            if message["action"] == "done":
                break
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

    def action(self, name: str, schema: Any = None) -> (
        Callable[
            [
                Callable[[dict], Awaitable[None]],
            ],
            Callable[[dict], Awaitable[None]]
            ]):
        """
        Automatically detects whether func is an async function, in which case
        the method is a regular action, or whether it is an asynchronous
        generator, in which case we are dealing with a conversation type action
        """
        def decorator(func):
            if schema:
                if isasyncgenfunction(func):
                    self._conversation_prototypes[name] = schemator(schema, func)
                else:
                    self._actions[name] = schemator(schema, func)
            else:
                if isasyncgenfunction(func):
                    self._conversation_prototypes[name] = func
                else:
                    self._actions[name] = func
            return func
        return decorator

    async def converse(self, message, prototype):
        """
        Manually starts a conversation with a partner without necessarily
        having a prototype, allowing "anonymous" conversations. As well as
        spawning a conversation even if the action was originally a primitive
        action.
        """
        if prototype in self._conversation_prototypes:
            generator = self._conversation_prototypes[prototype](message)
            await self._start_conversation(generator)
        else:
            generator = prototype(message)
            await self._start_conversation(generator)

    def conversation(self, name):
        """ 
        Explicitly adds a generator to the appropriate conversation dictionary
        """
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

    async def spawn(self,
                    factory: str,
                    message: dict,
                    same_shard: bool=True, # Ignored for now
                    name: str=None,
                    add_monitor: bool = True):

        """
        This method spawns an actor using the given factory,
        sending the prescribed message to it.
        """
        args = {}
        if name:
            args["name"] = name
        if add_monitor:
            args["add_monitor"] = self.name

        if self._shard:
            await self.send(self._shard, {"action": "spawn",
                                     "type": factory,
                                     "message": message,
                                     **args
                                     })

    async def replace(self,
                      factory: Callable[["Actor", dict], Awaitable[None]],
                      message: dict) -> None:
        """
        Replaces the current actor's behaviors with another. Also replaces
        the state encapsulating closure. It does this by running the factory
        function of the new actor after having cleared the _actions dict.
        """
        self._actions.clear()
        self._default_actions()
        for task in self._daemon_tasks.values():
            task.cancel()    
        self._daemons.clear()
        self._daemon_tasks.clear()
        if self._on_stop:
            await self._on_stop()
        self._on_stop = None
        self._on_start = None

        for conversation in self._conversations:
            await self.send(conversation.partner, {"action": "end_conversation",
                                                   "cid": conversation.cid})
            await conversation.close()
        self._conversations.clear()

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
        self._default_actions()
        self._message_task = self.loop.create_task(self._process_messages())
        self._main_sentinel = await self._sentinel(self._message_task,
                                                   self._monitors,
                                                   "main_task",
                                                   exceptions=True,
                                                   cancellations=True,
                                                   done=True)
        #self._message_task.add_done_callback(self._error_callback)

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
                print(traceback.print_exc())#exception)
                raise exception
            else:
                pass
        else:
            pass

    async def _sentinel(self,
                        task,
                        monitors,
                        task_tag,
                        exceptions = True,
                        cancellations=False,
                        done = False):
        """
        Spawns a task to watch over task and report to its monitors when the
        task is finished for one reason or another (typically exceptions)
        """
        async def reporter():
            try:
                await task
            except BaseException as e:
                print(traceback.format_exc())
                #print(f"Exception: {e}")
            if task.cancelled():
                print("Cancelled")
                if cancellations:
                    for monitor in monitors:
                        await self.send(monitor, {"action": "sentinel",
                                                  "type": "cancellation",
                                                  "address": self.name,
                                                  "tag": task_tag})
                else:
                    pass
            elif task.exception():
                print("Excepted")
                if exceptions:
                    for monitor in monitors:
                        await self.send(monitor, {"action": "sentinel",
                                                  "type": "exception",
                                                  "address": self.name,
                                                  "exception": str(task.exception()),
                                                  "tag": task_tag})
            else:
                print("Done")
                if done:
                    for monitor in monitors:
                        await self.send(monitor, {"action": "sentinel",
                                                  "type": "done",
                                                  "address": self.name,
                                                  "tag": task_tag})

        return self.loop.create_task(reporter())

    async def _monitor(self, message):
        """
        An action for adding a monitor to the actor
        """
        if "address" in message:
            self._monitors.add(message["address"])
        else:
            pass