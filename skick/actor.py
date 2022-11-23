"""
This module contains a class for actors. This is the fundamental building
block of Skick, and is the fundamental unit of computation in actor models.
"""
import asyncio
from typing import Callable, Awaitable, Any
from inspect import isasyncgenfunction
from random import choice

from schema import Schema

from .message_system_interface import MessageSystemInterface
from .cluster import cluster_data
from . import terminate


def schemator(schema, func):
    """
    Wraps an async function func in a schema so that it will only actually be
    called if the argument conforms to the schema.
    """
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

    When an actor is instantiated, it is, in a sense, *empty*. We need to
    attach functionality to it through the use of the included decorators.
    These register handlers that the actor will call under various
    circumstances, such as when it receives a message or when it is started.
    We could probably phrase this as a version of the builder pattern, but the
    required steps are intended to be collated in special functions that function
    like factories. In practice the user never directly instantiates an actor.

    Actors can hold state. The state is stored separately from the actual actor
    class. While this is not strictly necessary (the user could attach state
    directly to the actor instance), it is generally recommended that the state
    is stored in the closure of the factory function that produced the actor's
    *behaviors*. This has many advantages, including having the state be garbage
    collected when the actor replaces its behaviors.
    """

    def __init__(
        self,
        name: str,
        message_system: MessageSystemInterface = None,
        queue_size: int = 100,
        loop: asyncio.AbstractEventLoop = None,
        shard: str = None,
    ) -> None:
        self.name = name
        self.queue = asyncio.Queue(queue_size)
        self.loop = loop or asyncio.get_event_loop()
        self._actions = {}
        self._conversation_prototypes = {}
        self._daemons = {}
        self._daemon_watchers = {}
        self._on_start = None
        self._on_stop = None

        self._daemon_tasks = {}
        self._daemon_watcher_tasks = {}
        self._message_task = None
        self._mailman_cleanup = None
        self._message_system = message_system
        self._shard = shard
        self._conversations = {}

        self._main_sentinel = None
        self._monitors = set()

        self._replacement_state = -1
        self._init_lock = asyncio.Lock()

        self._injected_spawn = None
        self._sentinel_handlers = {}
        self._default_sentinel = None
        self.stopped = None

    def get_service(self, name, local="mixed"):
        """
        Retrieves a provider for some service from the local cluster_data
        singleton.
        """
        try:
            match local:
                case "mixed":
                    return choice(
                        list(cluster_data.local_services.get(name, None))
                        or list(cluster_data.remote_services.get(name, None))
                    )

                case "local":
                    return choice(list(cluster_data.local_services.get(name, None)))

                case "remote":
                    return choice(list(cluster_data.remote_services.get(name, None)))

                case _:
                    return None
        except TypeError:
            return None

    def register_service(self, name):
        """Registers the current actor as a service with the given name."""
        cluster_data.add_service(name, self.name)

    def unregister_services(self):
        """Unregisters all services this actor provides."""
        cluster_data.delete_service(self.name)

    def sentinel_handler(self, address, handler):
        """Registers a handler for a sentinel message"""
        self._sentinel_handlers[address] = handler

    def default_sentinel(self, handler):
        """A decorator for registering a default sentinel handler"""
        self._default_sentinel = handler
        return handler

    def _default_actions(self):
        """Adds default actions"""
        self.action("monitor")(self._monitor)
        self.action("sentinel")(self._receive_sentinel_info)

    async def _receive_sentinel_info(self, message):
        """
        This is a message handler for receiving notifications of monitored actors
        dying. It attempts to invoke handlers registered for a particular actor.
        If it can not find a handler for the specific actor, it falls back to a
        default handler.
        """
        sender = message.get("address", None)
        if sender in self._sentinel_handlers:
            await self._sentinel_handlers[sender](message)
        elif self._default_sentinel:
            await self._default_sentinel(message)
        else:
            pass

    async def _start_conversation(self, generator):
        """
        This method is invoked when an action indicates that it is in fact
        a conversation type action. It then returns an async generator, the first
        element of which is a Conversation object, which contains the necessary
        information to manage the conversation. We simply store it in the
        _conversations dictionary after having performed some initial setup.
        """
        conversation = await generator.asend(None)
        greeting = await conversation.attach(generator, self.name)
        self._conversations[conversation.cid] = conversation
        if greeting:
            # In this case the object is a *call* and ret is the message we need to
            # send to the other party to get the conversation started
            await self.send(conversation.partner, greeting)

        else:
            # In this case, we are dealing with a respond action, where the
            # actor is expecting to deliver the first reply. In this case, we
            # simply need to reiterate as the message will come later.
            # N.B: We may also receive SocketQueries here, and other exotic
            # objects. We need to handle this separately. Until we are ready to rip the whole system out,
            # we will simply use inheritance to allow us to handle this case.

            response = await generator.__anext__()
            response.cid = conversation.cid
            await self._wrap_response(response, conversation)

    async def _wrap_response(self, response, conversation):
        await self.send(
            conversation.partner,
            {
                "action": "receive_reply",
                "cid": conversation.cid,
                "sender": self.name,
                "message": response,
            },
        )

    async def _process_reply(self, message: dict) -> None:
        """
        This method interfaces with the conversation object. It ensures that
        incoming messages are sent to the correct conversation, that responses
        are expedited and that conversations end when they are supposed to.
        """
        if message["action"] == "end_conversation":
            await self._end_conversation(message)
        elif message["action"] == "receive_reply":
            conversation = self._conversations[message["cid"]]
            reply = await conversation(message)
            await self._reply_parser(reply)

    async def _reply_parser(self, reply):
        """A small helper for parsing incoming replies"""
        if reply:
            (destination, reply) = reply
            await self.send(destination, reply)
            if reply["action"] == "end_conversation":
                await self._end_conversation(reply)
        else:
            pass

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

            action = message.get("action", None)
            if action == "done":
                break
            if action in self._actions:
                await self._actions[action](message)

            elif action in self._conversation_prototypes:
                generator = self._conversation_prototypes[action](message)
                if generator:
                    await self._start_conversation(generator)
                else:
                    pass
            elif action in ["receive_reply", "end_conversation"]:
                if message["cid"] in self._conversations:
                    await self._process_reply(message)
                else:
                    pass  # Ignore nonexistent conversations
            else:
                pass

    def action(
        self, name: str, schema: Any = None
    ) -> (
        Callable[
            [
                Callable[[dict], Awaitable[None]],
            ],
            Callable[[dict], Awaitable[None]],
        ]
    ):
        """
        This is a decorator for registering an "action", that is, a behavior
        to be executed when a particular type of message is received. The
        "behavior" is simply an async function that we register in the appropriate
        field in the instance.

        It automatically detects whether func is an async function, in which case
        the method is a regular action, or whether it is an asynchronous
        generator, in which case we are dealing with a conversation type action.
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
        having a preregisterd prototype, allowing "anonymous" conversations.

        In order to start a conversation this way, supply a message and an
        async prototype(msg) function. The prototype function must be an async
        generator. And must yield a Call object at the start of the conversation.
        This is when the recipient and the actual initial message are decided.
        """
        if prototype in self._conversation_prototypes:
            generator = self._conversation_prototypes[prototype](message)
            await self._start_conversation(generator)
        else:
            generator = prototype(message)
            await self._start_conversation(generator)

    def conversation(self, name):
        """
        Explicitly adds a generator to the appropriate conversation dictionary.
        """

        def decorator(gen):
            self._conversation_prototypes[name] = gen
            return gen

        return decorator

    def daemon(self, name, dead_mans_switch=True, restart=False, on_failure=None):
        """
        A decorator for registering daemon that will be started as a task when
        the actor starts, and which will be cancelled when the actor stops or is
        replaced.

        There are a few different ways to handle failures and cancellations:

        1. Dead Man's Switch: If the daemon terminates, the entire actor will
           die with it. This is the default behavior.
        2. Restart: If the daemon terminates, it will be restarted.
        3. Ignore: Do nothing

        Additionally we may supply a *callback*. If the daemon terminates, the
        on_failure method will be called if it was supplied to the decorator.
        This happens regardless of the behavior chosen.
        """

        def decorator(func):
            self._daemons[name] = func
            if any((dead_mans_switch, restart, on_failure)):

                async def watcher():
                    if dead_mans_switch and not restart:
                        try:
                            await self._daemon_tasks[name]
                        except asyncio.CancelledError:
                            raise
                        except:
                            # Here, we have to provide a blanket except clause
                            # so that the exception doesn't kill the watcher
                            # task itself. In the future we might do something
                            # constructive with it.
                            pass

                        if on_failure:
                            await on_failure()
                        await self.stop()

                    elif restart:
                        while True:
                            try:
                                await self._daemon_tasks[name]
                            except asyncio.CancelledError:
                                raise
                            except:
                                # Here, we have to provide a blanket except clause
                                # so that the exception doesn't kill the watcher
                                # task itself. In the future we might do something
                                # constructive with it.
                                pass

                            if on_failure:
                                await on_failure()
                            self._daemon_tasks[name] = self.loop.create_task(func())

                    elif on_failure:
                        await self._daemon_tasks[name]
                        await on_failure()

                self._daemon_watchers[name] = watcher
            else:
                self._daemon_watchers[name] = None
            return func

        return decorator

    def on_start(self, func):
        """
        A decorator that registers a method which will be run asynchronously when
        the actor is started.
        """

        self._on_start = func
        return func

    def on_stop(self, func):
        """
        A decorator that registers a method which will be run asynchronously
        when the actor is stopped.
        """
        self._on_stop = func
        return func

    async def spawn(
        self,
        factory: str,
        message: dict,
        remote: str = "mixed",
        name: str = None,
        add_monitor: bool = True,
    ):

        """
        This method spawns an actor using the given factory, sending the
        prescribed message to it. In practice this can only be used if the shard
        object has registered a spawn method with the actor. This will be the
        case for all normal actors the user defines.
        """
        if add_monitor:
            if isinstance(add_monitor, bool):
                monitors = [self.name]
            elif isinstance(add_monitor, str):
                monitors = [add_monitor]
            elif isinstance(add_monitor, list):
                monitors = add_monitor
            else:
                monitors = None
        else:
            monitors = None

        if self._injected_spawn:
            return await self._injected_spawn(
                factory, message=message, remote=remote, name=name, monitors=monitors
            )
        else:
            raise RuntimeError(f"No spawn function has been injected into {self.name}")

    def _start_daemons(self):
        """Starts all daemons registered with the actor."""
        daemon_pairs = zip(self._daemons.items(), self._daemon_watchers.items())
        for (name, daemon), (_, watcher) in daemon_pairs:
            self._daemon_tasks[name] = self.loop.create_task(daemon())
            if watcher:
                self._daemon_watchers[name] = self.loop.create_task(watcher())
            else:
                self._daemon_watchers[name] = None

    def _stop_daemons(self):
        """Stops all daemons registered with the actor."""
        current = asyncio.current_task()
        for (name, watcher) in self._daemon_watchers.items():
            if watcher:
                watcher.cancel()
        for (name, daemon) in self._daemon_tasks.items():
            if not daemon == current:
                daemon.cancel()

        self._daemon_watchers.clear()
        self._daemon_tasks.clear()
        return current

    async def _replace(
        self,
        factory: Callable[["Actor", dict], Awaitable[None]],
        message: dict,
        injected_cleanse=None,
        injected_populate=None,
    ) -> None:
        """
        Replaces the current actor's behaviors with another. Also replaces
        the state encapsulating closure. It does this by running the factory
        function of the new actor after having cleared the _actions dict and
        some other internal dictionaries.

        The replacement operation is somewhat problematic. This is because it
        can be invoked not only as a result of an incoming message. Therefore,
        we are not guaranteed sequential execution, and we need to be careful
        not to cause various race conditions.

        The solution chosen is as follows: The clearing and repopulating of
        the actor is protected by a mutex. This is necessary since there are
        asynchronous operations in the process which may yield control to a
        different task wich may simultaneously perform the same operation.
        If we lock this part of the process, we can guarantee that the outcome
        is deterministic.

        Once all the old state has been cleared out, we need to run the _on_start
        method and start any daemons. This step has previously caused some
        problems. For example, in some cases daemons could be started twice
        if replacement occurred in the _on_start method. In order to avoid this,
        we use the following strategy: We keep track of something called a
        replacement state. Whenever a task acquires the _init_lock mutex, it
        sets the state to 0, then, before releasing the mutex, it increases the
        state to 1. It will run the daemons only if the state is still 1 after
        the on_start method. If this was the case, it will set the state to 2
        to prevent other tasks from running the same daemons.
        """

        # The following is needed to prevent daemons from cancelling themselves
        is_daemon = asyncio.current_task() in self._daemon_tasks.values()

        if isinstance(factory, str):
            factory = cluster_data.local_factories[factory][1]

        async with self._init_lock:
            self._replacement_state = 0
            self._actions.clear()

            if injected_cleanse:
                await injected_cleanse
            if injected_populate:
                await injected_populate

            self._default_actions()

            task = self._stop_daemons()

            self._daemons.clear()

            if self._on_stop:
                await self._on_stop()
            self._on_stop = None
            self._on_start = None

            for conversation in self._conversations:
                con = self._conversations[conversation]
                await self.send(
                    con.partner, {"action": "end_conversation", "cid": con.cid}
                )
                await con.close()
            self._conversations.clear()

            self._default_sentinel = None
            self._sentinel_handlers.clear()

            factory(self, message)
            self._replacement_state = 1

        # Here we must explain a pertinent detail. From lines 19 and 20 of
        # Lib/asyncio/locks.py in the python source code, we can see that the
        # context handler will not yield control to the event loop within the
        # async __aexit__ method. Therefore, the program runs synchronously from
        # self._replacement_state = 1 to the if self._on_start line. This means
        # we do not need to assure ourselves that another task has not gotten
        # halfway through its instance variable carnage phase before we get to
        # our on_start method.
        if self._on_start:
            await self._on_start()

        # However, at this point, we may have seen additional replace
        # operations between the time we called self._on_start() and now.
        if self._replacement_state == 1:
            # Beneath there is an esoteric idea. Since we will be reading
            # the daemons from self, it does not matter if *this* task's
            # replace operation ran a different factory's self._on_start method
            # we will always be reading the correct daemons here even if the
            # associated factory function and _on_start methods were ran in a
            # different task. Therefore, we choose to run the daemons as soon
            # as possible, and then never again.
            self._replacement_state = 2
            self._start_daemons()

        if is_daemon:
            task.cancel()

    async def replace(
        self, factory, message, injected_cleanse=None, injected_populate=None
    ):
        """
        Replaces an actor with the specified actor type. See actor._replace for
        details.

        This method protects the _replace method so that it can be run from
        a daemon. With this mechanism, the daemon can avoid breaking the actor
        when performing replacements.
        """
        shielded_task = asyncio.shield(
            self._replace(
                factory,
                message,
                injected_cleanse=injected_cleanse,
                injected_populate=injected_populate,
            )
        )
        await shielded_task

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
        return await self._message_system.send(address, message)

    async def run(self) -> None:
        """
        Runs the tasks associated with the actor.
        """

        await self.mailman()

        if self._on_start:
            await self._on_start()

        # Only do this if no replacement action has been initiated in the
        # _on_start method. If we didn't ensure this, we would start them twice.
        if self._replacement_state == -1:
            self._replacement_state = 2
            self._start_daemons()

        self._default_actions()
        self._message_task = self.loop.create_task(self._process_messages())

        self._main_sentinel = self._sentinel(
            self._message_task,
            self._monitors,
            "main_task",
            exceptions=True,
            cancellations=True,
            done=True,
        )

    async def _stop(self) -> None:
        """Kills the actor and cleans up"""
        if self._message_task:
            self._message_task.cancel()

        self._stop_daemons()

        if self._on_stop:
            await self._on_stop()

        if self._mailman_cleanup:
            await self._mailman_cleanup()

    async def stop(self):
        """
        Launuches the _stop operation as a separate task and registers it
        with the terminate singleton. This is necessary because we may need
        to perform the stop when terminating the shard. If so, we need to know
        be able to ensure that the actors get a chance to run their on_stop
        methods before we terminate the shard.
        """
        if not self.stopped:
            fut, self.stopped = terminate.add_stopper(self._stop(), self.loop)
            await fut

    def _sentinel(
        self,
        task,
        monitors,
        task_tag,
        stop=True,
        exceptions=True,
        cancellations=False,
        done=False,
    ):
        """
        Spawns a task to watch over another task and report to its monitors when the
        task is finished.
        """

        async def reporter():
            await asyncio.gather(task, return_exceptions=True)

            if stop:
                await self.stop()
            else:
                pass
            if task.cancelled():
                if cancellations:
                    for monitor in monitors:
                        await self.send(
                            monitor,
                            {
                                "action": "sentinel",
                                "type": "cancellation",
                                "address": self.name,
                                "tag": task_tag,
                            },
                        )
                else:
                    pass
            elif task.exception():
                if exceptions:
                    for monitor in monitors:
                        await self.send(
                            monitor,
                            {
                                "action": "sentinel",
                                "type": "exception",
                                "address": self.name,
                                "exception": str(task.exception()),
                                "tag": task_tag,
                            },
                        )
            else:
                if done:
                    for monitor in monitors:
                        await self.send(
                            monitor,
                            {
                                "action": "sentinel",
                                "type": "done",
                                "address": self.name,
                                "tag": task_tag,
                            },
                        )

        return self.loop.create_task(reporter())

    async def _monitor(self, message):
        """
        An action for adding a monitor to the actor. A different actor can
        send us a message following the schema:

        {"action": "monitor", "address": [address of caller]}

        The actor will then remember to notify the requesting party when it
        dies.
        """
        if "address" in message:
            self._monitors.add(message["address"])
        else:
            pass
