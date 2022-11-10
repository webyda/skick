"""
This file contains a websocket manager actor, which is responsible for
listening on a websocket through an interface, and spawning handler actors
to incoming connections. It is also responsible for setting up *session* actors
wich contain the actual business logic of the socket endpoints.

It does this by running a factory function which extracts functions, schemas
and closures and assembles the final actors.
"""

import asyncio
from typing import Callable
from secrets import token_hex
from orjson import JSONDecodeError, dumps, loads

from schema import Schema, Const, And, Or

from .actor import Actor
from .websocket_interface import WebsocketConnectionObject, WebsocketServerInterface
from .message_system_interface import MessageSystemInterface

from .addressing import get_address
from .rate_limiter import TokenBucket, RateExceeded



class SocketQuery:
    """
    A class for signaling to the syzygy that a conversation between the back
    actor and some other actor is halted because the back actor requests a
    reply from the websocket client before it can continue.
    """

    def __init__(self, message, subschema):
        self.message = message
        self.subschema = subschema


class FrontActor(Actor):
    """
    A specialized actor class for websocket receptionist actors. These actors
    are instantiated by the special Websocket Actor whenever a new connection
    is opened by a client. They are responsible for filtering messages from the
    client and sending them to a corresponding session actor for processing.
    """

    def __init__(self, name, message_system=None, shard=None, loop=None):
        super().__init__(name, message_system=message_system, shard=shard, loop=loop)

        self._websocket = None
        self.associate = None
        self._socket_schemas = {}
        self._replacement_factories = {}  # To deserialize factories

        self._default_actions()
        self._socket_task = None
        self._socket_sentinel = None

        self._query_schemas = {}
        self._queries = {}
        self._default_schemas()

        self._ws_limiter = TokenBucket()

    def _default_actions(self):
        """
        Adds default behaviors for accepting communications from the assoicated
        session actor.
        """
        super()._default_actions()
        super().action("socksend")(self.socksend)
        super().action("replace_from_message")(self.replace_from_message)
        super().action("new_query")(self._new_query)

    def _default_schemas(self):
        """
        Adds some default subschemas to the front actor for standard query
        types. This eliminates the need to define new queries for common types
        of messages and replies from the client, so as to reduce clutter in
        downstream applications.
        """
        self._socket_schemas["query_reply"] = Schema(
            {"action": Const("query_reply"), "query_id": str, "message": object}
        )
        self.query_schema("default", Or(str, int, float, bool))
        self.query_schema("str", str)
        self.query_schema("int", int)
        self.query_schema("float", float)
        self.query_schema("bool", bool)
        self.query_schema("list", [str, int, float, bool])

    def query_schema(self, name, schema):
        """ Registers a schema to be used for some SocketQuery. """
        self._query_schemas[name] = Schema(schema)

    async def _process_reply(self, message):
        """
        Processes a reply from the client to a specific query. There are a
        variety of things which have not yet been checked by the ordinary system.
        Specifically, we need to ensure the message portion of the reply is
        consistent with the provided subschema, and that there is an actual
        active query with the given ID.
        """
        if message["query_id"] in self._queries:
            subschema = self._queries[message["query_id"]]
            if subschema.is_valid(message["message"]):
                await self.send(
                    self.associate,
                    {
                        "action": "query_reply",
                        "query_id": message["query_id"],
                        "message": message["message"],
                    },
                )
                del self._queries[message["query_id"]]
            else:
                await self.socksend({"error": "Invalid subschema"})
        else:
            pass
            # Ignore this error

    async def _new_query(self, message):
        """
        A message handler for setting up client queries for the websocket.
        """
        if message["schema"] in self._query_schemas:
            self._queries[message["query_id"]] = self._query_schemas[message["schema"]]
            await self.socksend(message["message"])
        else:
            self._queries[message["query_id"]] = self._query_schemas["default"]
            await self.socksend(message["message"])

    async def replace_from_message(self, message):
        """
        Receives a message from the associated session actor and performs
        the replacement therein specified.
        """
        factory = self._replacement_factories[message["factory"]]
        await self.replace(factory, message["message"])

    async def _replace_cleanse(self):
        """
        Adds a step to the replacement process so that we can clear the
        websocket specific stuff.
        """
        self._socket_schemas.clear()
        self._query_schemas.clear()

    async def _replace_populate(self):
        """
        Adds a step to the replacement process so that we can populate the
        websocket specific dictionaries.
        """
        self._default_schemas()

    def on_start(self, func):
        """
        We wish to ignore on_start and on_stop reserving them for the back
        actor.
        """
        return func

    def on_stop(self, func):
        """
        We wish to ignore on_start and on_stop reserving them for the back
        actor.
        """
        return func

    def front_on_start(self, func):
        """
        Allow the receptionist to be assigned startup actions.
        """
        return super().on_start(func)

    def front_on_stop(self, func):
        """
        Allows the receptionist to be assigned stop actions.
        """
        return super().on_stop(func)

    def action(self, name, schema=None):
        """
        We do not wish to register actions by default in front actors.
        This is because we would prefer if "socket" methods were set up
        as syzygy actions, while "action" methods refer to the session actor
        and allows it to interact with ordinary actors.
        """

        def decorator(func):
            return func

        return decorator

    def daemon(self, name):
        """
        Prevent daemons from running on the front actor by default
        """

        def decorator(func):
            return func

        return decorator

    def front_daemon(self, name):
        """
        Allows running a daemon specifically on the front actor.
        """
        return super().daemon(name)

    def front_action(self, name, schema=None):
        """
        In some cases, we may be interested in specifically assigning an action
        to the receptionist actor. In these rare cases, we can invoke this
        method to specify that we wish to assign an action to the receptionist.
        """
        return super().action(name)

    def socket(self, name, schema):
        """
        This decorator acts a bit like the action decorator for normal actors.
        The difference is that it defines a schema such that if a message arrives
        on the websocket that conforms to the schema, then the message is sent
        to the session actor which processes the message in accordance with the
        provided method.
        
        In the FrontActor, this merely records the schema.
        """

        def decorator(func):
            validator = Schema({**schema, **{"action": Const(name)}}).is_valid
            self._socket_schemas[name] = validator
            return func

        return decorator

    async def socksend(self, message):
        """Sends a message over the websocket if there is one."""
        if self._websocket:
            await self._websocket.send(dumps(message["message"]))

    async def socklistener(self):
        """Listens to the websocket"""
        async for message in self._websocket:
            try:
                waited = await self._ws_limiter.waiter()
            except:
                await self.stop()
                break
            try:
                message = loads(message)
            except JSONDecodeError:
                await self.socksend({"message": {"action": "error"}})
                continue

            if "action" in message:
                action = message["action"]
                if action in self._socket_schemas:
                    valid = self._socket_schemas[action](message)
                    if valid:
                        if not action == "query_reply":
                            await self.send(self.associate, message)
                        else:
                            await self._process_reply(message)
                    else:
                        await self.socksend(
                            {
                                "message": {
                                    "action": "error",
                                    "type": "not schema compliant",
                                }
                            }
                        )
                else:
                    await self.socksend(
                        {
                            "message": {
                                "action": "error",
                                "type": f"No schema present for action '{action}'",
                            }
                        }
                    )
            else:
                await self.socksend(
                    {"message": {"action": "error", "type": "No action specified"}}
                )

        await self.stop()

    async def run(self):
        """ Starts the actor and starts listening to the websocket. """
        await super().run()
        self._socket_task = self.loop.create_task(self.socklistener())
        self._socket_sentinel = await self._sentinel(
            self._socket_task,
            {self.associate},
            "socket_task",
            exceptions=True,
            cancellations=True,
            done=False,
        )
        self._monitors.add(self.associate)

    async def _stop(self):
        """Stops the websocket listener"""
        if self._socket_task:
            self._socket_task.cancel()
        await super()._stop()


class BackActor(Actor):
    """
    This class is a companion class to the FrontActor class. It is intended to
    receive those messages from the websocket that the FrontActor has verified
    as schema compliant. It then performs some action on them. Apart from this
    it has helper methods for communicating with the websocket via the FrontActor.
    """

    def __init__(self, name, message_system=None, shard=None, loop=None):
        super().__init__(name, message_system=message_system, shard=shard, loop=loop)
        self._replacement_factories = {}  # A dictionary used for serializing
        self.associate = None
        self._socket_queries = {}

    def socket(self, name, schema):
        """
        This decorator is a companion to the socket decorator in the FrontActor.
        When the session factory function is run on the BackActor, it ignores the
        schema, but records the function as a standard action. This allows the
        FrontActor to send it approved messages from the websocket, acting only
        on those that are schema compliant.
        """
        return self.action(name)

    async def _sock_query_handler(self, message):
        """
        Handles replies to a conversation from a socket to fill a request by
        injecting them in the normal conversation handler as if they came from
        """
        if message["query_id"] in self._socket_queries:
            qid = message["query_id"]
            cid = self._socket_queries[qid]["cid"]
            msg = message["message"]
            del self._socket_queries
            await self._process_reply(
                {"action": "receive_reply", "cid": cid, "message": msg}
            )
        else:
            pass

    async def socksend(self, message):
        """Sends a message to the websocket via the front actor"""
        await self.send(self.associate, {"action": "socksend", "message": message})

    async def _replace_cleanse(self):
        """
        We wish to add a query clearing step in the replacement process. This
        will take place within the locked part of the process.
        """
        self._socket_queries = {}

    async def _replace_populate(self):
        """
        We need to send a message to the front actor to tell it to replace its
        schemas. This needs to happen before the on_start method is run, so
        it is entirely appropriate to do it here. Due to the lock, the last
        replacement message sent corresponds to the last replacement operation.
        RabbitMQ also ensures messages will be delivered in order. Therefore,
        we will always have a consistent state eventually.

        Nota bene: We have previously sent the message to the front actor after
        the replacement operation. This causes a bug since the back actor may
        run another replacement operation in the on_start method, causing
        chains of replacements where the last one will take effect on the
        back actor, and the first one on the front actor. Therefore the order of
        operations is essential.
        """
        await self.send(
            self.associate,
            {"action": "replace_from_message", "factory": factory, "message": message},
        )

    async def replace(self, factory, message):
        """
        The replacement method needs to be adapted to websocket syzygies.
        Replacement always happens in pairs, where the session actor alters
        its behavior first, and then requests the receptionist actor to change
        its schema specifications.

        Nota bene: Here the order of operations matters. The replacement
        message must be sent to the front actor before we perform the
        replacement locally. If we do not do this, then the back actor can
        perform a second replacement within the super().replace() call
        because of the @on_start decorator. If this happens, then the back
        actor replacements will take immediate effect (because they are
        directly applied) but the front actor replacements will have to wait.
        Additionally, the front actor replacements will arrive in the reverse
        order, and the final front actor will not match the back actor. This
        bug has occurred in the past.
        """
        await super().replace(self._replacement_factories[factory], message)

    async def _reply_parser(self, reply):
        """
        This is a slightly ugly hack to allow the session actor to intercept
        queries meant for the websocket client, allowing it to set up a
        pipeline for sending the query to the client and receiving it back
        before the conversation resumes.

        The schema is presumed to be presupplied in the receptionist through
        a separate function. It will therefore have a fixed set of templates
        for response schemas. Any messages which do not conform to these
        subschemas will simply be rejected.
        """
        if isinstance(reply, SocketQuery):
            query_id = token_hex()
            self._socket_queries[query_id] = reply.cid

            await self.send(
                self.associate,
                {
                    "action": "new_query",
                    "message": reply.message,
                    "query_id": query_id,
                    "schema": str,
                },
            )
        else:
            await super()._reply_parser(reply)

    def front_action(self, name, schema=None):
        """
        Ignore actions meant for the receptionist actor
        """

        def decorator(func):
            return func

        return decorator

    def front_daemon(self, name):
        """Ignores daemons specifically meant to run on the front actor"""

        def decorator(func):
            return func

        return decorator

    def front_on_start(self, func):
        """
        Ignore start actions meant for the receptionist actor
        """
        return func

    def front_on_stop(self, func):
        """
        Ignore stop actions meant for the receptionist actor
        """
        return func

    async def run(self):
        """
        Starts the actor, adding a sentinel for the receptionist actor on top
        of the regular startup function.
        """
        await super().run()
        self._monitors.add(self.associate)


def WebsocketActor(
    socket_interface: WebsocketServerInterface,
    messaging_system: Callable[[], MessageSystemInterface],
    shard,
    loop=None,
):
    """
    Creates a Websocket Handler Actor. Its job is to maintain the websocket server,
    spawning new syzygies of actors to handle connections. It also supervises
    these actors. And maintains registries of session types and handshake procedures.
    """
    sock = socket_interface

    session_types = {}

    subsession_types = {}
    reverse_subsessions = {}

    socket_actor = Actor(
        get_address(), message_system=messaging_system(), shard=shard, loop=loop
    )

    receptionists = {}
    session_actors = {}

    def subsession(name):
        """
        A decorator for sessions. It allows the programmer to write a syzygy
        of a session actor and its receptionist, with a fully functional
        websocket attached using a declarative definition in an async function.

        In practice, this works by running the decorated function several times
        but feeding it instances of different actor classes where the
        relevant methods have different functionality.
        """

        def decorator(func):
            subsession_types[name] = func
            reverse_subsessions[func] = name
            return func

        return decorator

    def session(name):
        """
        Similar to the subsession decorator, but also registers the session type in a separate
        dictionary for behaviors choosable by websocket clients. This is
        necessary to prevent the client from choosing inappropriate behaviors,
        such as an intermediate actor syzygy used in a login process.

        The separation between sessions and subsessions therefore allows us
        to specify that some sessions types must only be used as a result
        of replacement of a legitimate session syzygy.
        """

        def decorator(func):
            subsession(name)(func)
            session_types[name] = func
            return func

        return decorator

    def handshake(func):
        """
        Takes a factory function and generates a handshake sequence. The idea
        is that the handshake object works on a higher level of abstraction, such as
        logging into an account and initializing its session state based on some
        database state, and that this process occurs as a sequence of actors.
        The handshake sequence carries the name of a final actor type with it
        which will be replaced in the final replacement operation. The handshake
        handler ensures that the first actor in the sequence is properly registered
        and provides a convenient decorator for denoting an actor factory as a
        subsession, resulting from that specific handshake sequence.

        The sessions and subsessions in the sequence have to fulfill the
        following criteria:

        1. They must proliferate the field "session_type" in
           their replacement messages,
        2. The final subsession in the sequence must replace iteslf with the
           actor type found in the "session_type" field in their incoming
           replacement message, but with the appended string ":final"

        Example:

        ```
        @ws.handshake
        def authenticated(inst, message):
            ...

        @authenticated("user")
        def final_stage(inst, message):
            '''
            This actor factory will receive user information in its message
            and will only be reachable over websocket through a login procedure
            '''
            @inst.socket("whoami", {"action": "whoami"})
            async def whoami(msg):
                await inst.socksend({"name": message["name"]})
        ```
        This results in a chain of actors of the form

        user -> ... -> user:final
        """

        def decorator(name):
            def inner(inner_func):
                session(name)(func)
                subsession(f"{name}:final")(inner_func)
                return inner_func

            return inner

        return decorator

    socket_actor.handshake = handshake
    socket_actor.subsession = subsession
    socket_actor.session = session

    async def socket_handler(socket: WebsocketConnectionObject):
        """
        This handler is fed to the Websocket system to handle new connections.
        """
        try:
            message = loads(await socket.recv())
            if isinstance(message, dict) and "session_type" in message:
                handshake = message["session_type"]
            else:
                handshake = None
        except JSONDecodeError:
            handshake = None

        if handshake in session_types:
            # First we instantiate the actors and provide them with some
            # necessary information.
            factory = session_types[handshake]
            receptionist = FrontActor(
                get_address(), message_system=messaging_system(), shard=shard, loop=loop
            )
            session_actor = BackActor(
                get_address(), message_system=messaging_system(), shard=shard, loop=loop
            )

            receptionist._replacement_factories = subsession_types
            session_actor._replacement_factories = subsession_types

            receptionist._websocket = socket

            receptionist.associate = session_actor.name
            session_actor.associate = receptionist.name

            receptionist._injected_spawn = socket_actor._injected_spawn
            session_actor._injected_spawn = socket_actor._injected_spawn

            factory(receptionist, message)
            factory(session_actor, message)

            await receptionist.run()
            await session_actor.run()

            await receptionist._monitor(socket_actor.name)
            await session_actor._monitor(socket_actor.name)

            receptionists[receptionist.name] = receptionist
            session_actors[session_actor.name] = session_actor

            await asyncio.wait_for(receptionist._socket_task, None)

        else:
            await socket.send("denied")

    # Due to this being implemented in the style of an actor, employing our
    # usual closure based formulation, we can not simply overload the run
    # method to include launching the websocket listener in the class
    # definition. However, we can replace it on a per-instance basis. This is
    # a bit of a hack, but it works.

    old_run = socket_actor.run
    running_server = None

    async def new_run():
        """ A method for starting the Websocket actor """
        nonlocal running_server
        await old_run()
        running_server = await sock.serve(socket_handler)

    old_stop = socket_actor.stop

    async def new_stop():
        """ A method for stopping the websocket actor """
        running_server.close()
        for receptionist in receptionists.values():
            await receptionist.stop()
        for session_actor in session_actors.values():
            await session_actor.stop()
        await old_stop()

    socket_actor.run = new_run
    socket_actor.stop = new_stop

    @socket_actor.default_sentinel
    async def default_sentinel(msg):
        """
        A default sentinel for the websocket actor, allowing it to deal with
        dropped connections.
        """
        if msg["address"] in receptionists:
            del receptionists[msg["address"]]
        elif msg["address"] in session_actors:
            del session_actors[msg["address"]]

    return socket_actor
