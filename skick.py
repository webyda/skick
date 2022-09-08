"""
This module contains an abstraction of the Skick system so that the user
does not have to instantiate the various objects, manage their interrelations
and worry about things like which type of actor class is fed into the Shard
actor.

Instead, it provides a simple interface which, for unencrypted single core
instances, requires no parameters.
"""

import asyncio
from secrets import token_hex

# Import the messaging system interfaces
from simple_message import SimpleFactory
from rabbit_message import RabbitFactory
from combined_message import CombinedFactory


# Import all distributed dictionaries
from simple_hash import SimpleHash
from redis_hash import RedisHash

# Import all websocket implementations
from plain_websocket import PlainWebsocket

# Import our various actors 
from actor import Actor
from websocket_actor import WebsocketActor
from shard import Shard


class Skick:
    """
    A class which acts as an Adapter, presenting a simple, convenient interface
    to the underlying reified closures, objects, etc. 
    """
    def __init__(self, on_start, *args, **kwargs):
        self.on_start = on_start
        
        if args:
            self.on_stop = args[0]
        else:
            self.on_stop = None
        
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        if "dict_type" in kwargs:
            if isinstance(kwargs["dict_type"], str):
                self.redis_url = kwargs["dict_type"]
                self.hash = RedisHash
            else:
                pass
        else:
            self.hash = SimpleHash
        self.hash.loop=self.loop
        print(kwargs)
        if "message_system" in kwargs:
            if isinstance(kwargs["message_system"], str):
                self.msg_sys = RabbitFactory(kwargs["message_system"],
                                             self.loop).create
                print("Selected RabbitMessage")
            else:
                pass
        else:
            print("Selected SimpleMessage")
            self.msg_sys = SimpleFactory({}).create
            
        if "shard_name" in kwargs:
            name = kwargs["shard_name"]
        else:
            name = None
            
        self.shard = Shard(Actor,
                           self.msg_sys,
                           name if name else token_hex(),
                           self.loop)
        
        self.name = self.shard.name
        
        
        if "websocket_host" in kwargs: 
            ws_host = kwargs["websocket_host"]
        else:
            ws_host = "localhost"
            
        if "websocket_port" in kwargs:
            ws_port = kwargs["websocket_port"]
        else:
            ws_port = 8000
            
        if "websocket_server" in kwargs:
            if not kwargs["websocket_server"]:
                self.ws_actor = None
            else:
                self.ws_actor = None
        else:
            self.ws_actor = WebsocketActor(PlainWebsocket(ws_host, ws_port),
                                           self.msg_sys,
                                           shard=self.shard.name,
                                           loop=self.loop)
            
        if self.ws_actor:
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
        """ Allows us to run the actor system """
        if self.hash == RedisHash:
            await RedisHash.set_pool(self.redis_url)
        else:

            pass
        
        self.tasks["shard"] = await self.shard.run()
        
        if self.ws_actor:
            self.tasks["websocket"] = await self.ws_actor.run()
        else:
            self.tasks["websocket"] = None
            
        await self.on_start(self.shard)
    
    def start(self):
        self.loop.create_task(self._run())
        self.loop.run_forever()
        
    async def stop(self):
        """ Allows us to stop the actor system """
        await self.on_stop()
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
    