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
from secrets import token_urlsafe
from json import JSONDecodeError, dumps, loads

from schema import Schema, Const

from actor import Actor
from websocket_interface import WebsocketConnectionObject, WebsocketServerInterface
from message_system_interface import MessageSystemInterface


class FrontActor(Actor):
    """ A specialized actor class for websocket receptionist actors """
    def __init__(self, name, message_system=None, loop=None):
        super().__init__(name, message_system=message_system, loop=loop)

        self._websocket = None
        self.associate = None
        self._socket_schemas = {}
        self._replacement_factories = {}  # To deserialize factories

        self._default_actions()
        self._socket_task = None

    def _default_actions(self):
        """
        Adds default behaviors for accepting communications from the assoicated
        session actor.
        """
        super().action("socksend")(self.socksend)
        super().action("replace_from_message")(self.replace_from_message)

    async def replace_from_message(self, message):
        """
        Receives a message from the associated session actor and performs
        the replacement therein specified.
        """
        factory = self._replacement_factories[message["factory"]]
        await self.replace(factory, message["message"])

    async def replace(self, factory, message):
        self._socket_schemas.clear()
        await super().replace(factory, message)
        self._default_actions()  # Re-add default actions

    def action(self, name):
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
     
    def front_action(self, name):
        """
        In some cases, we may be interested in specifically assigning an action
        to the receptionist actor. In these rare cases, we can invoke this
        method to specify that we wish to assign an action to the receptionist.
        """
        return super().action(name)

    def socket(self, name, schema):
        """ Reads the schema and name and deposits them. """
        def decorator(func):
            validator = Schema({**schema, **{"action": Const(name)}}).is_valid
            self._socket_schemas[name] = validator
            return func
        return decorator

    async def socksend(self, message):
        """ Sends a message over the websocket if there is one. """
        if self._websocket:
            await self._websocket.send(dumps(message))

    async def socklistener(self):
        """ Listens to the websocket """
        async for message in self._websocket:
            try:
                message = loads(message)
            except JSONDecodeError:
                await self.socksend({"action": "error"})
                continue

            if "action" in message:
                action = message["action"]
                if action in self._socket_schemas:
                    valid = self._socket_schemas[action](message)
                    if valid:
                        await self.send(self.associate, message)
                    else:
                        await self.socksend({"action": "error", "type": "not schema compliant"})
                else:
                    await self.socksend({"action": "error", "type": f"No schema present for action '{action}'"})
            else:
                await self.socksend({"action": "error", "type": "No action specified"})

    async def run(self):
        await super().run()
        self._socket_task = self.loop.create_task(self.socklistener())

    async def stop(self):
        """ Stops the websocket listener """
        if self._socket_task:
            self._socket_task.cancel()
        await super().stop()


class BackActor(Actor):
    """
    This class implements an actor type for managing messages entering the
    system from a websocket. It receives filtered and verified messages from
    a trusted specialized actor, and proceeds to do useful work.
    """
    def __init__(self, name, message_system=None, loop=None):
        super().__init__(name, message_system=message_system, loop=loop)
        self._replacement_factories = {}  # A dictionary used for serializing
        self.associate = None

    def socket(self, name, schema):
        """
        Decorates an async function from a session declaration, extracting the
        body as a handler for the messages coming from the socket.
        """
        return self.action(name)

    async def replace(self, factory, message):
        """
        The replacement method needs to be adapted to websocket syzygies.
        Replacement always happens in pairs, where the session actor alters
        its behavior first, and then requests the receptionist actor to change
        its schema specifications.
        """
        await super().replace(factory, message)

        await self.send(self.associate,
                        {"action": "replace_from_message",
                         "factory": self._replacement_factories[factory],
                         "message": message})

    def front_action(self, name):
        """
        Ignore actions meant for the receptionist actor
        """
        def decorator(func):
            return func
        return decorator
    
    def front_daemon(self, name):
        """ Ignores daemons specifically meant to run on the front actor """
        def decorator(func):
            return func
        return decorator


def WebsocketActor(socket_interface: WebsocketServerInterface,
                   messaging_system: Callable[[], MessageSystemInterface], loop=None):
    """ Creates a Websocket Handler Actor. """
    sock = socket_interface

    session_types = {}

    subsession_types = {}
    reverse_subsessions = {}

    socket_actor = Actor(token_urlsafe(),
                         message_system=messaging_system(),
                         loop=loop)

    subactors = set()

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
        Same as subsession, but also registers the session type in a separate
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
            receptionist = FrontActor(token_urlsafe(),
                                      message_system=messaging_system(),
                                      loop=loop)
            session_actor = BackActor(token_urlsafe(),
                                      message_system=messaging_system(),
                                      loop=loop)

            receptionist._replacement_factories = subsession_types
            session_actor._replacement_factories = reverse_subsessions

            receptionist._websocket = socket

            receptionist.associate = session_actor.name
            session_actor.associate = receptionist.name

            factory(receptionist, message)
            factory(session_actor, message)

            await receptionist.run()

            await session_actor.run()

            subactors.add((receptionist, session_actor))
            
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
        nonlocal running_server
        await old_run()
        running_server = await sock.serve(socket_handler)

    old_stop = socket_actor.stop

    async def new_stop():
        nonlocal subactors
        running_server.close()
        for (front, back) in subactors:
            await front.stop()
            await back.stop()

        await old_stop()

    socket_actor.run = new_run
    socket_actor.stop = new_stop

    return socket_actor
