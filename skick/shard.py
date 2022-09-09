"""
Contains a function that generates a shard actor. This is a factory function
that takes a concrete actor type and attaches the sufficient behaviors for it
to manage spawning, deletion and distribution of new actors over a cluster.

The instances of the shard actor also serve as decorators with which we
register child actor factories and associate them with message templates.
"""
import asyncio
import traceback

from random import random, sample
from math import log
from secrets import token_urlsafe
from typing import Callable, Type, Awaitable
from itertools import groupby, chain
from functools import reduce

from schema import Const, Optional

from .message_system_interface import MessageSystemInterface
from .actor import Actor

def encode(shard,
           local_services,
           remote_services,
           inactive_remote_services,
           remote_shards,
           inactive_shards):
    """ Simply prerpares a dictionary for transmission """
    services = (local_services.keys()
                | remote_services.keys() 
                | inactive_remote_services.keys())
    
    new_remote_serv = {key: list(merge(local_services, remote_services, key))
                       for key in services}
    
    local_shard = list(set(remote_shards[shard]
                           if shard in remote_shards
                           else [])
                       | set(sum(map(list, local_services.values()), [])))
    
    # Since the shard does not keep track of inactive local service actors,
    
    return {"services": new_remote_serv,
            "dead_services": {key: list(val) for key, val
                              in inactive_remote_services.items()},
            "shards": {**{shard_name: list(subactors) for (shard_name, subactors) in remote_shards.items()},
                       shard: local_shard},
            "dead_shards": inactive_shards,
            }
             
def merge(dict1, dict2, key):
    """ Merges the contents from dict1 and dict2 as sets at the key key """
    A = set(dict1[key]) if key in dict1 else set()
    B = set(dict2[key]) if key in dict2 else set()
    return A | B

def shard(actor_base: Type[Actor],
          message_factory: Callable[[], Awaitable[MessageSystemInterface]],
          shard_id: str = None,
          loop = None) -> Actor:
    """
    Takes a concrete actor type and returns an actor
    """
    
    msg_sys = message_factory()
    shard_actor = actor_base(shard_id if shard_id else token_urlsafe(),
                             msg_sys, loop=loop)
    
    factories = {}  # This keeps track of all available factory functions
    actors = {}  # This keeps track of all managed actor instances


    local_services = {}  # This keeps track of all local services
    
    remote_services = {}  # This keeps track of all remote services
    inactive_remote_services = {}
    
    remote_shards = {}
    inactive_shards = {}  # A set of formerly active shards
            
            
    @shard_actor.daemon("whisperer")
    async def whisperer(whisper_interval=1):
        """
        Regularly whispers known information to adjacent shards
        the algorithm picks random shards from the list of remote shards
        and sends one copy of the data to a shard with a lower address
        and one copy to a shard with a higher address. This is repeated with
        a set time interval. This ensures topological mixing and allows us to
        concentrate the data at the edges of the address space
        """
        while True:
            await asyncio.sleep(whisper_interval)
            
            lower = [shard for shard in remote_shards
                     if shard < shard_actor.name]
            upper = [shard for shard in remote_shards
                     if shard > shard_actor.name]
            
            
            recip = chain(sample(lower, k=min(len(lower),
                                              int(log(len(remote_shards)+1)))),
                          sample(upper, k=min(len(upper),
                                              int(log(len(remote_shards)+1)))))
                       
            for recipient in recip:
                await shard_actor.send(recipient, {"action": "receive_info",
                                                **encode(shard_actor.name,
                                                            local_services,
                                                            remote_services,
                                                            inactive_remote_services,
                                                            remote_shards,
                                                            inactive_shards),
                                                "purge": False,
                                                "broadcaster": shard_actor.name})
    
    @shard_actor.daemon("broadcaster")
    async def broadcaster():
        """
        Regularly broadcasts known information to all shards using the
        message system's broadcast functionality. This is done with some
        probability which is inversely proportional to the rank the shard has in
        the address space of its neighbors.
        """
        while True:
            await asyncio.sleep(1)
            # use some smart distribution in the future. For now, just use a
            # uniform distribution

            if random() < 1/(len(remote_shards)+1):
                await msg_sys.broadcast({"action": "receive_info",
                                        **encode(shard_actor.name,
                                                local_services,
                                                remote_services,
                                                inactive_remote_services,
                                                remote_shards,
                                                inactive_shards),
                                        "purge": True,
                                        "broadcaster": shard_actor.name})
            else:
                pass

    @shard_actor.action("register_service", schema={"action": Const("register_service"),
                                                    "service": str,
                                                    "address": str})
    async def register(message):
        """
        Allows actors to register themselves as providing a service. 
        Schema:
        {"action": "register_service",
         "service": [name of service],
         "address": [address of actor]}
        """
        if message["service"] in local_services:
            local_services[message["service"]].add(message["address"])
        else:
            local_services[message["service"]] = {message["address"]}

    @shard_actor.action("unregister_service", schema={"action": Const("unregister_service"),
                                                      "actor": str})
    async def unregister(message):
        """
        unregisters a previously registered service
        """
        nonlocal local_services
        nonlocal inactive_remote_services
        for service in local_services:
            if message["actor"] in local_services[service]:
                local_services[service].remove(message["actor"])
                inactive_remote_services[service].add(message["actor"])
            else:
                pass

    
    @shard_actor.action("request_service", schema={"action": Const("request_service"),
                                                   "service": str,
                                                   "sender": str})
    async def request_service(message):
        """
        Requests a service from the service records of the actor
        Schema:
        {"action": "request_service",
         "service": [name of service],
         "sender": [reply address]}
        """
        serv = message["service"]
        local = list(local_services[serv]) if serv in local_services else []
        remote = list(remote_services[serv]) if serv in remote_services else []

        await shard_actor.send(message['sender'], {"action": "service_delivery",
                                                   "service": message["service"],
                                                   "local": local,
                                                   "remote": remote})
        
    @shard_actor.action("receive_info", schema={"action": Const("receive_info"),
                                                "services": dict,
                                                "dead_services": dict,
                                                "shards": dict,
                                                "dead_shards": dict,
                                                "purge": bool,
                                                Optional("broadcaster"): str,})
    async def infodump(message):
        """
        Allows the actor to receive updated information from other actors
        
        Expects a dictionary on the form
        {
         services: {[service_name]: [addresses]},
         dead_services: {[service_name]: [addresses]}},
         shards: {[shard_name]: [addresses]},
         dead_shards: {[shard_name]: [addresses]},
        }
        We use this to construct a new set of local data for the service
        registry. We can use set operations on dictionary keys to combine
        dictionaries.
        
        Also ameliorates the problem of reintroduction of dead actors, since
        the individual shard knows whether its own actors are still alive,
        effectively allowing it to reintroduce the deadness check.
        """
        nonlocal remote_services
        nonlocal inactive_remote_services
        nonlocal remote_shards
        nonlocal inactive_shards

        # First we look if the broadcaster believes we possess a service
        # which does not exist. It has been purged but is still known of as 
        # active, so it needs to be reintroduced as dead.
        all_local = reduce(set.union, local_services.values(), set())
        all_remote = reduce(set.union, remote_services.values(), set())
        
        inc_shard = set(message["shards"][shard_actor.name]) if shard_actor.name in message["shards"] else set()
        
        diff = inc_shard-all_local
        
        
        #First we collect a setlike collection of all known service categories
        services = (message["services"].keys() 
                    | message["dead_services"].keys()
                    | remote_services.keys()
                    | inactive_remote_services.keys())

        tmp_remote = {}
        tmp_dead = {}
        
        purge = set()
        # Then we iterate over the known services, constructing sets for active
        # and inactive providers as we go along.
        
        for service in services:
            locals = set(local_services[service]) if service in local_services else set()
            mserv = set(message["services"][service]) if service in message["services"] else set()
            dserv = set(message["dead_services"][service]) if service in message["dead_services"] else set()
            rserv = set(remote_services[service]) if service in remote_services else set()
            iserv = set(inactive_remote_services[service]) if service in inactive_remote_services else set()
            
            if message["purge"]:
                # If we are purging, we remove all services mentioned as dead,
                # but we make an exception for local services which are known
                # to still be alive. We trust the local shard to have better
                # information on its own services than the rest of the network.
                purge.update(dserv-locals)
            else:
                pass
            
            # We combine the various sets of services into sets of active
            # remote shards and inactive remote shards.
            remote = mserv|rserv
            dead = dserv|iserv
            remote -= dead | locals
            
            tmp_remote[service] = remote
            tmp_dead[service] = dead

        # Next we update our lists of shards and their service actors
        shards = message["shards"].keys() | remote_shards.keys()
        dead_shards = message["dead_shards"].keys() | inactive_shards.keys()
        
        shards -= dead_shards
        
        shards = {key: merge(message["shards"], remote_shards, key)
                  for key in shards}
        
        dead_shards = {key: merge(message["dead_shards"], inactive_shards, key)
                       for key in dead_shards}
        
        # Next, we update the state variables to reflect our new knowledge
        
        remote_services = tmp_remote
        remote_shards = shards
        if not message["purge"]:
            inactive_remote_services = tmp_dead
            inactive_shards = dead_shards
        else:
            for service in inactive_remote_services:
                inactive_remote_services[service]-=purge
                
                
        
        
    async def spawn(actor_type, message={}, name=None, run=True):
        if actor_type in factories:
            actor_type, actor_factory = factories[actor_type]

            name = name if name else token_urlsafe()

            actor = actor_type(name,
                               message_system=message_factory(),
                               loop=loop,
                               shard=shard_actor.name)
            actor_factory(actor, message)

            actors[name] = actor
            if run:
                await actor.run()
                return actor
            else:
                return actor

        else:
            return None
    
    shard_actor.spawn = spawn
        
    @shard_actor.action("spawn", schema={"action": Const("spawn"),
                                         "type": str,
                                         Optional("name"): str,
                                         Optional("message"): dict,
                                         Optional("add_monitor"): str,
                                         })
    async def spawn_from_message(message):
        """
        Instantiates an actor of the relevant type as found in the factories
        dictionary.
        """
        act_type = message["type"]
        name = message["name"] if "name" in message else token_urlsafe()
        msg = message["message"] if "message" in message else {}
        act = await spawn(act_type, message=msg, name=name, run=False)
        
        if "add_monitor" in message:
            await act._monitor({"address": message["add_monitor"]})
            await act.run()
        else:
            await act.run()
            

    def factory(name, actor_type=actor_base):
        """ Attaches a factory to the shard """
        def decorator(func):
            factories[name] = (actor_type, func)
            return func
        return decorator

    shard_actor.actor = factory  # Exposes an interface to the programmer
    
    @shard_actor.on_start
    async def onstart():
        await msg_sys.register_shard(shard_actor.name)
    
    @shard_actor.on_stop
    async def onstop():
        """ Broadcasts the shutdown to the rest of the cluster. """
        msg = {
            "action": "receive_info",
            **encode(shard_actor.name,
                     {},
                     {},
                     local_services,
                     {},
                     {shard_actor.name: list(reduce(set.union,
                                               local_services.values(),
                                               set()))}),
            "purge": True,
            "broadcaster": shard_actor.name,
            }
        await msg_sys.broadcast(msg)
        
    return shard_actor


Shard = shard

    
