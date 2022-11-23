"""
This file contains a websocket manager actor, which is responsible for
listening on a websocket through an interface, and spawning handler actors
to incoming connections. It is also responsible for setting up *session* actors
wich contain the actual business logic of the socket endpoints.

It does this by running a factory function which extracts functions, schemas
and closures and assembles the final actors, putting them in a syzygy object
to keep track of them.
"""
import asyncio
from typing import Callable
from orjson import JSONDecodeError, loads

from .actor import Actor
from .websocket_interface import WebsocketConnectionObject, WebsocketServerInterface
from .message_system_interface import MessageSystemInterface

from .addressing import get_address

from .front_actor import FrontActor
from .back_actor import BackActor
from .syzygy import Syzygy


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
    Syzygy.WebsocketException = socket_interface.get_exception_class()

    session_types = {}

    subsession_types = {}
    reverse_subsessions = {}

    socket_actor = Actor(
        get_address(), message_system=messaging_system(), shard=shard, loop=loop
    )

    syzygies = {}

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

            await receptionist._monitor({"address": socket_actor.name})
            await session_actor._monitor({"address": socket_actor.name})
            syzygy = Syzygy(receptionist, session_actor, receptionist._socket_task)

            syzygies[receptionist.name] = syzygy
            syzygies[session_actor.name] = syzygy

            await syzygy.supervise()
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
        """A method for starting the Websocket actor"""
        nonlocal running_server
        await old_run()
        running_server = await sock.serve(socket_handler)

    old_stop = socket_actor.stop

    async def new_stop():
        """A method for stopping the websocket actor"""
        running_server.close()
        for name, syzygy in syzygies.items():
            await syzygy.stop()

        await old_stop()

    socket_actor.run = new_run
    socket_actor.stop = new_stop

    @socket_actor.default_sentinel
    async def default_sentinel(msg):
        """
        A default sentinel for the websocket actor, allowing it to deal with
        dropped connections.
        """
        addr = msg.get("address", None)
        syzygy = syzygies.get(addr, None)
        if syzygy:
            await syzygy.sentinel_handler(msg)
            addrs = syzygy.addresses
            for addr in addrs:
                del syzygies[addr]

    return socket_actor
