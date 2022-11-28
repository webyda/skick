"""
This module contains an abstraction of the Skick system so that the user
does not have to instantiate the various objects, manage their interrelations
and worry about things like which type of actor class is fed into the Shard
actor.

Instead, it provides a simple interface which, for unencrypted single core
instances, requires no parameters.
"""


import asyncio
from ssl import create_default_context, Purpose


# There are two platform dependent details in this module.
# 1. The uvloop module is not available on Windows, so the user must
#    run the standard asyncio event loop instead.
# 2. Signals do not exist on Windows and loop.add_signal_handler() is not
#    available. Instead, we rely on python's emulated verison in signal.signal
from platform import system
if system() != "Windows":
    from signal import SIGINT, SIGTERM, SIGHUP
    SIGNALS = [SIGINT, SIGTERM, SIGHUP]
    import uvloop
    new_event_loop = uvloop.new_event_loop
else:
    from signal import SIGINT, SIGTERM, signal
    SIGNALS = [SIGINT, SIGTERM]
    new_event_loop = asyncio.new_event_loop
    
from .addressing import get_address

# Import the messaging system interfaces
from .simple_message import SimpleFactory

# from .rabbit_message import RabbitFactory
# from .combined_message import CombinedFactory
from .routed_message import RoutedFactory

# Import all distributed dictionaries
from .simple_hash import SimpleHash
from .redis_hash import RedisHash

# Import all websocket implementations
from .plain_websocket import PlainWebsocket

# Import our various actors
from .actor import Actor
from .websocket_actor import WebsocketActor
from .shard import Shard

# import the stopping mechanism
from . import terminate
from .cluster import cluster_data


class Skick:
    """
    A class which acts as an Adapter, presenting a simple, convenient interface
    to the underlying reified closures, objects, etc.
    """

    def __init__(self, *args, on_start=None, **kwargs):
        self.on_start = on_start
        if args:
            self.on_stop = args[0]
        else:
            self.on_stop = None

        self.loop = new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.switch = terminate.set_loop(self.loop)

        if "dict_type" in kwargs:
            if isinstance(kwargs["dict_type"], str):
                self.redis_url = kwargs["dict_type"]
                self.hash = RedisHash
            else:
                pass
        else:
            self.hash = SimpleHash
        self.hash.loop = self.loop

        if "message_system" in kwargs:
            if isinstance(kwargs["message_system"], str):
                self.msg_sys = RoutedFactory(kwargs["message_system"], self.loop).create
            else:
                pass
        else:
            self.msg_sys = SimpleFactory({}).create

        self.shard = Shard(Actor, self.msg_sys, get_address(is_shard=True), self.loop)
        self.shard.ws_actor = None
        self.name = self.shard.name
        cluster_data.shard = self.name

        ws_host = kwargs.get("websocket_host", "localhost")
        ws_port = kwargs.get("websocket_port", 8000)

        if "websocket_server" in kwargs:
            self.ws_actor = None

        else:

            ssl_context = kwargs.get("ssl", None)
            if isinstance(ssl_context, str):
                """In this case, we have been given a pem file"""
                pem = ssl_context
                ssl_context = create_default_context(Purpose.CLIENT_AUTH)
                ssl_context.load_cert_chain(pem)

            elif isinstance(ssl_context, tuple):
                """IN this case we have a pem file and a password"""
                pem, pwd = ssl_context
                ssl_context = create_default_context(Purpose.CLIENT_AUTH)
                ssl_context.load_cert_chain(certfile=pem, password=pwd)

            ws_opts = {
                "ssl": ssl_context,
                "max_size": 2**16,
                "max_queue": 2**3,
                "read_limit": 2**16,
                **kwargs.get("websocket_options", {}),
            }
            self.ws_actor = WebsocketActor(
                PlainWebsocket(ws_host, ws_port, **ws_opts),
                self.msg_sys,
                shard=self.shard.name,
                loop=self.loop,
            )

        if self.ws_actor:
            self.ws_actor._injected_spawn = self.shard._injected_spawn
            self.session = self.ws_actor.session
            self.subsession = self.ws_actor.subsession
            self.handshake = self.ws_actor.handshake

        else:

            def tmpsess(name):
                def decorator(func):
                    return func

                return decorator

            def tmphandshake(func):
                return func

            self.session = tmpsess
            self.subsession = tmpsess
            self.handshake = tmphandshake

        self.actor = self.shard.actor

        self.tasks = {}

    async def _run(self):
        """Allows us to run the actor system"""
        if self.hash == RedisHash:
            await RedisHash.set_pool(self.redis_url)
        else:
            pass

        if self.ws_actor:
            self.shard.ws_actor = self.ws_actor
        self.tasks["shard"] = await self.shard.run()

        if self.ws_actor:
            self.tasks["websocket"] = await self.ws_actor.run()
        else:
            self.tasks["websocket"] = None

        if self.on_start:
            await self.on_start(self.shard)

    def start(self):
        """Manages the startup process and adds some error handling"""

        def signal_handler(*args):
            self.loop.create_task(self.stop())

        if system() != "Windows":
            for sig in SIGNALS:
                self.loop.add_signal_handler(sig, signal_handler)
            self.loop.create_task(self._run())
            self.loop.run_until_complete(self.switch)
        else:
            for sig in SIGNALS:
                signal(sig, signal_handler)
            self.loop.create_task(self._run())
            self.loop.run_until_complete(self.switch)
            
    async def stop(self):
        """Allows us to stop the actor system"""
        if self.on_stop:
            await self.on_stop()
        if self.ws_actor:
            self.shard.ws_actor = None
            await self.ws_actor.stop()

        await self.shard.stop()

    def add_directory(self, directory):
        """
        Takes a directory which has ostensibly been created in a separate
        module and adds its sessions, subsessions and actors to the system.
        """
        for name, func in directory.sessions.items():
            self.session(name)(func)

        for name, func in directory.subsessions.items():
            self.subsession(name)(func)

        for name, func in directory.actors.items():
            self.actor(name)(func)
