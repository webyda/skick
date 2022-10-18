"""
Contains a function that generates a shard actor. This is a factory function
that takes a concrete actor type and attaches the sufficient behaviors for it
to manage spawning, deletion and distribution of new actors over a cluster.

The instances of the shard actor also serve as decorators with which we
register child actor factories and associate them with message templates.
"""
import asyncio

from random import choice, random, sample
from math import log
from typing import Callable, Type, Awaitable
from itertools import chain
from functools import reduce
from time import time, perf_counter

from schema import Schema, Const, Optional, Or

from .message_system_interface import MessageSystemInterface
from .actor import Actor
from .conversation import Respond
from .info_bucket import InfoBucket
from .addressing import get_address

    
def shard(actor_base: Type[Actor],
          message_factory: Callable[[], Awaitable[MessageSystemInterface]],
          shard_id: str = None,
          loop = None) -> Actor:
    """
    Takes a concrete actor type and returns an actor
    """
    
    msg_sys = message_factory()
    shard_actor = actor_base(shard_id if shard_id else get_address(is_shard=True),
                             msg_sys, loop=loop)

    factories = {}  # This keeps track of all available factory functions
    actors = {}  # This keeps track of all managed actor instances

    infos = InfoBucket()

    local_services = {}  # This keeps track of all local services
    remote_services = {}  # This keeps track of all remote services
    remote_shards = {}
    remote_factories = {} # Keeps track of which shards can spawn which actors
            
    
    def update_bucket():
        """ Updates the info bucket with local information """
        timestamp = time()
        for service in local_services:
            for provider in local_services[service]:
                infos.add_information({"key": {"shard": shard_actor.name,
                                               "service": service,
                                               "address": provider},
                                       "timestamp": timestamp,
                                       "info": True})
        
        infos.add_information({"key": {"shard": shard_actor.name},
                               "timestamp": timestamp,
                               "info": True})
        
        for factory in factories:
            infos.add_information({"key": {"shard": shard_actor.name,
                                           "factory": factory},
                                   "timestamp": timestamp,
                                   "info": True})
    
    def decode_bucket():
        """ Extracts information from the bucket to update local values """
        nonlocal remote_services
        nonlocal remote_shards
        nonlocal remote_factories
        def filter_remote_services(info):
            """ True for keys representing remote services """
            key, (_, alive) = info
            return "service" in key and alive and not key["shard"] == shard_actor.name
        
        def map_remote_services(info):
            key, _ = info
            return (key["service"], key["address"])
        
        remote_services = infos.extract(filter_func=filter_remote_services,
                                        map_func=map_remote_services,
                                        to_dict=True,
                                        listify=True)
        
        def filter_shards(info):
            """ Returns true for keys representing shards other than the current one"""
            key, (_, alive) = info
            return ("shard" in key
                    and len(key) == 1
                    and not key["shard"] == shard_actor.name
                    and alive)
            
        def map_shard(info):
            """ Produces a dictionary of active and inactive shards """
            key, (_, alive) = info
            return key["shard"]
        
        shards = set(infos.extract(filter_func=filter_shards,
                                        map_func=map_shard,
                                        to_dict=False))
        def filter_shard_services(info):
            """ True for active services on nonlocal shards """
            key, (_, alive) = info
            return ("service" in key
                    and key["shard"] in shards
                    and alive)
        def map_shard_services(info):
            """ Maps the information to a dictionary of shards and services """
            key, _ = info
            return (key["shard"], key["service"])
        
        remote_shards = infos.extract(filter_func=filter_shard_services,
                                      map_func=map_shard_services,
                                      to_dict=True,
                                      listify=(shards or True))
        
        def filter_factories(info):
            """ True for active factories on living shards """
            key, _ = info
            return ("factory" in key
                    and not key["shard"] == shard_actor.name
                    and key["shard"] in shards)
            
        def map_factories(info):
            """ Maps the information to a dictionary of factories """
            key, _ = info
            return key["factory"], key["shard"]
        
        remote_factories = infos.extract(filter_func=filter_factories,
                                         map_func=map_factories,
                                         to_dict=True,
                                         listify=True)
        
        # Finally, attempt to detect wether there is any false information about
        # our current shard in the bucket. If so, replace it with an up to date
        # correction. Specifically, try to detect if there are any services
        # marked as alive which are actually dead.
        timestamp = time()
        def filter_erroneous_locals(info):
            """
            Filter out actors on our shard which are maked as alive, but
            which are, in fact, not present.
            """
            key, (_, alive) = info
            return (key["shard"] == shard_actor.name
                    and "service" in key
                    and key["address"] not in local_services[key["service"]]
                    and alive)
        def map_corrections(info):
            """ Constructs a set of dictionaries with corrected information """
            key, _ = info
            return {"key": key,
                    "timestamp": timestamp,
                    "info": False}
        
        corrections = infos.extract(filter_func=filter_erroneous_locals,
                                    map_func=map_corrections,
                                    to_dict=False)
        infos.consume_batch(corrections)
        
    @shard_actor.daemon("whisperer")
    async def whisperer(whisper_interval=1):
        """
        Regularly whispers known information to adjacent shards
        the algorithm picks random shards from the list of remote shards
        and sends one copy of the data to a shard with a lower address
        and one copy to a shard with a higher address. This is repeated with
        a set time interval. This ensures topological mixing and allows us to
        concentrate the data at the edges of the address space.
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
            update_bucket()
            info_batch = infos.export()
            for recipient in recip:
                await shard_actor.send(recipient, {"action": "receive_info",
                                                   "batch": info_batch,
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
                update_bucket()
                info_batch = infos.export()
                await msg_sys.broadcast({"action": "receive_info",
                                         "batch": info_batch,
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

        for service in local_services:
            if message["actor"] in local_services[service]:
                local_services[service].remove(message["actor"])
                infos.add_information({"key": {"shard": shard_actor.name,
                                               "service": service,
                                               "address": message["actor"]},
                                       "timestamp": time(),
                                       "info": False})
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
        
    query_schema = Schema({"action": Const("query_service"),
                           "message": str,
                           "sender": str,
                           "cid": str}).is_valid
    
    @shard_actor.action("query_service")
    async def query_service(msg):
        yield Respond(msg)
        if query_schema(msg):
            serv = msg["message"]
            local = list(local_services[serv]) if serv in local_services else []
            remote = list(remote_services[serv]) if serv in remote_services else []
            yield {"local": local, "remote": remote}
        
        else:
            yield {"error": "invalid query"}

    @shard_actor.action("receive_info", schema={"action": Const("receive_info"),
                                                "batch": [{"key": dict, "timestamp": float, "info": Or(bool, dict)}],
                                                "purge": bool,
                                                "broadcaster": str})
    async def receive_info(message):
        """
        Receives information from other shards and updates the information
        records accordingly. If the purge flag is set, the information records
        are purged of all data on deceased nodes that are older than some
        number of seconds (presumably no nodes should believe they are still
        alive at this point). Purged obituaries will be reintroduced by
        the concerned shards if they receive a notice that they possess a service
        that they know not to be present.
        """
        infos.consume_batch(message["batch"])
        if message["purge"]:
            infos.purge(time() - 5)
        decode_bucket()
        
                
    async def spawn(actor_type, message={}, name=None, monitors = None, run=True):
        if actor_type in factories:
            actor_type, actor_factory = factories[actor_type]
            name = name if name else get_address()

            actor = actor_type(name,
                               message_system=message_factory(),
                               loop=loop,
                               shard=shard_actor.name)
            actor._factories = factories
            actor._injected_spawn = injectable_spawn
            actor_factory(actor, message)

            actors[name] = actor
            if monitors:
                if isinstance(monitors, str):
                    await actor._monitor({"address": monitors})
                else:
                    for m in monitors:
                        await actor._monitor({"address": m})
            if run:
                await actor.run()
                return name
            else:
                return name

        else:
            print(f"Actor type {actor_type} not found in {factories}")
            return None
    
    shard_actor.spawn = spawn
    
    async def injectable_spawn(actor_type, message={}, name=None, monitors=None, remote="mixed", run=True):
        """
        To be injected into actors to allow them to spawn on the shard without
        having to go through the messaging system. This is necessary for a few
        reasons, the most important being that the actor needs to be able to
        figure out the address of the new actor without having to wait for
        another message to arrive. Since the shard can know in advance what the
        name of the new actor will be, this is a good compromise.
        """
        if remote == "local":
            return await spawn(actor_type, message, name, monitors, run)
        elif remote == "mixed":
            if actor_type in factories:
                return await spawn(actor_type, message, name, monitors, run)
            elif actor_type in remote_factories:
                shard = choice(remote_factories[actor_type])
                name = get_address(shard=shard)
                await shard_actor.send(shard, {"action": "spawn",
                                               "type": actor_type,
                                               "name": name,
                                               "message": message,
                                               "add_monitor": monitors,
                                               "remote": "local",})
                return name
            else:
                print(f"No remote factory named {actor_type} in dictionary {remote_factories}")
        elif remote == "remote":
                shard = choice(remote_factories[actor_type])
                name = get_address(shard=shard)
                await shard_actor.send(shard, {"action": "spawn",
                                               "type": actor_type,
                                               "name": name,
                                               "message": message,
                                               "add_monitor": monitors,
                                               "remote": "local",})
                return name
        else:
            print("remote parameter is ill formed")
            return None
    
    shard_actor._injected_spawn = injectable_spawn
    
    @shard_actor.action("spawn" ,schema={"action": Const("spawn"),
                                         "type": str,
                                         Optional("name"): str,
                                         Optional("message"): dict,
                                         Optional("add_monitor"): Or([str], str),
                                         Optional("remote"): str,
                                         })
    async def spawn_from_message(message):
        """
        Instantiates an actor of the relevant type as found in the factories
        dictionary.
        """
        act_type = message["type"]
        name = message["name"] if "name" in message else get_address()
        msg = message["message"] if "message" in message else {}
        remote = message["remote"] if "remote" in message else "mixed"
        
        match remote:
            case "mixed":
                if act_type in factories:
                    await spawn(act_type, message=msg, name=name, monitors=message["add_monitor"])
                elif act_type in remote_factories:
                    await shard_actor.send(choice(remote_factories[act_type]),
                                           {**message, "remote": "local"})
                    
                else:
                    print(f"Incorrect actor type {act_type} requested")
                    
            case "local":
                if act_type in factories:
                    await spawn(act_type, message=msg, name=name, monitors=message["add_monitor"])
                else:
                    print(f"Incorrect actor type {act_type}")
            case "remote":
                if act_type in remote_factories:
                    await shard_actor.send(choice(remote_factories[act_type]),
                                           {**message, "remote": "local"})
                else:
                    print(f"Incorrect actor type {act_type}")
        
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

    
