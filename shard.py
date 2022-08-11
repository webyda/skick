"""
Contains a function that generates a shard actor. This is a factory function
that takes a concrete actor type and attaches the sufficient behaviors for it
to manage spawning, deletion and distribution of new actors over a cluster.

The instances of the shard actor also serve as decorators with which we
register child actor factories and associate them with message templates.
"""
import asyncio
from random import random, sample
from math import log
from secrets import token_urlsafe
from typing import Callable, Type, Awaitable
from itertools import groupby, chain

from message_system_interface import MessageSystemInterface
from actor import Actor

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
            "shards": {**remote_shards, shard: local_shard},
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
    info_counter = 0 # Used to provide information to other shards of whether
                     # they have current or outdated information about some
                     # particular service.
    
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
    async def whisperer(whisper_interval=.1):
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
                                        "broadcaster": shard_actor.name})
            else:
                pass

    @shard_actor.action("register_service")
    async def register(message):
        """
        Allows actors to register themselves as providing a service
        """
        if message["service"] in local_services:
            local_services[message["service"]].add(message["address"])
        else:
            local_services[message["service"]] = {message["address"]}

    @shard_actor.action("unregister_service")
    async def unregister(message):
        """
        unregisters a previously registered service
        """
        nonlocal local_services
        nonlocal inactive_remote_services
        if message["actor"] in local_services:
            for service in local_services:
                service -= message["actor"]
                inactive_remote_services[service].add(message["actor"])
        else:
            pass
    
    @shard_actor.action("request_service")
    async def request_service(message):
        """ Requests a service from the service records of the actor """
        serv = message["service"]
        local = list(local_services[serv]) if serv in local_services else []
        remote = list(remote_services[serv]) if serv in remote_services else []

        await shard_actor.send(message['sender'], {"action": "service_delivery",
                                                   "service": message["service"],
                                                   "local": local,
                                                   "remote": remote})
        
    @shard_actor.action("receive_info")
    async def infodump(message):
        """
        Allows the actor to receive updated information from other actors
        
        Expects a dictionary of the form
        {services: {[service_name]: [addresses]},
         dead_services: {[service_name]: [addresses]}},
         shards: {[shard_name]: [address]},
         dead_shards: {[shard_name]: [address]},
        }
        We use this to construct a new set of local data for the service
        registry. We can use set operations on dictionary keys to combine
        dictionaries.
        """
        nonlocal remote_services
        nonlocal inactive_remote_services
        nonlocal remote_shards
        nonlocal inactive_shards
        
        #First we collect a setlike collection of all known service categories
        services = (message["services"].keys() 
                    | message["dead_services"].keys()
                    | remote_services.keys()
                    | inactive_remote_services.keys())
        
        tmp_remote = {}
        tmp_dead = {}
        
        # Then we iterate over the known services, constructing sets for active
        # and inactive providers as we go along.
        for service in services:
            remote = ((set(message["services"][service]) 
                       if service in message["services"] else set()) 
                      | (remote_services[service]
                         if service in remote_services else set()))
            
            dead = ((set(message["dead_services"][service])
                     if service in message["services"] else set())
                    | (inactive_remote_services[service]
                       if service in inactive_remote_services else set()))
            
            remote -= dead | {shard_actor.name}
            
            dead -= {shard_actor.name}
            
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
        inactive_remote_services = tmp_dead
        remote_shards = shards
        inactive_shards = dead_shards
        
    @shard_actor.action("spawn")
    async def spawn(message):
        """
        Instantiates an actor of the relevant type as found in the factories
        dictionary.
        """
        if "type" in message and message["type"] in factories:
            actor_type, actor_factory = factories[message["type"]]

            name = message["name"] if "name" in message else token_urlsafe()

            actor = actor_type(name,
                               message_system=message_factory(),
                               loop=loop)
            actor_factory(actor, message)

            actors[name] = actor
            await actor.run()

        else:
            pass

    def factory(name, actor_type=actor_base):
        """ Attaches a factory to the shard """
        def decorator(func):
            factories[name] = (actor_type, func)
            return func
        return decorator

    shard_actor.actor = factory  # Exposes an interface to the programmer
    
    @shard_actor.on_start
    async def onstart():
        await msg_sys.register_shard(shard_actor)

    return shard_actor


Shard = shard

    
