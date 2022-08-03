"""
Contains a function that generates a shard actor. This is a factory function
that takes a concrete actor type and attaches the sufficient behaviors for it
to manage spawning, deletion and distribution of new actors over a cluster.

The instances of the shard actor also serve as decorators with which we
register child actor factories and associate them with message templates.
"""

from secrets import token_urlsafe
from typing import Callable, Type, Awaitable
from itertools import groupby, chain

from message_system_interface import MessageSystemInterface
from actor import Actor


def Shard(actor_base: Type[Actor],
          message_factory: Callable[[], Awaitable[MessageSystemInterface]],
          shard_id: str = None) -> Actor:
    """
    Takes a concrete actor type and returns an actor
    """
    shard_actor = actor_base(shard_id if shard_id else token_urlsafe(16),
                             message_factory())

    factories = {}  # This keeps track of all available factory functions
    actors = {}  # This keeps track of all managed actor instances


    local_services = {} # This keeps track of all local services
    remote_services = {} # This keeps track of all remote services
    remote_shards = [] # A sorted list of shards
    
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
            
        
    
    @shard_actor.daemon("broadcaster")
    async def broadcaster():
        """
        Regularly broadcasts known information to all shards using the
        message system's broadcast functionality. This is done with some
        probability which is inversely proportional to the rank the shard has in
        the address space of its neighbors.
        """
    
    service_validator = Schema({"action": Const("register_service"),
                                "service_name": Regex("^[a-zA-Z0-9_\-]{1,50}$"),
                                "address": Regex("^[a-zA-Z0-9_\-]{1,50}$")}).is_valid
    @shard_actor.action("register_service")
    async def register(message):
        """
        Allows actors to register themselves as providing a service
        """
        if service_validator(message):
            if message["service_name"] in local_services:
                local_services[message["service_name"]].add(message["address"])
            else:
                local_services[message["service_name"]] = {message["address"]}
        else:
            pass
    
    info_receiver_schema = Schema({"action": Const("receive_info"),
                                   "data": 
                                       {Regex("^[a-zA-Z_\-]{1,50}$"):
                                           {Regex("^[a-zA-Z_\-]{1,50}$"):
                                               lambda x: (isinstance(x, list)
                                                          and len(x) == 2
                                                          and isinstance(x[0], str)
                                                          and len(x[0]) <= 50
                                                          and isinstance(x[1], bool))
                                           }
                                       }
    }).is_valid
    @shard_actor.action("receive_info")
    async def infodump(message):
        """
        Allows the actor to receive updated information from other actors
        
        Expects a dictionary of the form
        {[name of service]: {address: (version, available)}},
        letting the receiver know whether the received information is newer
        (the version number is higher) and if the server has ceased to exist
        (available is False). The latter is used to distinguish between services
        unknown to the sender and services the sender knows to have ceased
        operations.
        """
        nonlocal remote_services
        if info_receiver_schema(message):
            data = message["data"]
            for service in data:
                if service in remote_services:
                    remote_services[service] = merge_info(data[service],
                                                          remote_services[service])
                    
        
    @shard_actor.action("spawn")
    async def spawn(message):
        """
        Instantiates an actor of the relevant type as found in the factories
        dictionary.
        """
        if "type" in message and message["type"] in factories:
            actor_type, actor_factory = factories[message["type"]]

            name = message["name"] if "name" in message else token_urlsafe(16)

            actor = actor_type(name, message_system=message_factory())
            await actor_factory(actor, message)

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

    return shard_actor

def join_dicts(dict1, dict2):
    """
    Takes two dicts and returns a full outer join of their items
    """
    keys1 = Set(dict1.keys())
    keys2 = Set(dict2.keys())
    
    left = keys1 - keys2
    right = keys2 - keys1
    intersection = keys1 & keys2
    
    return chain(((key, (dict1[key], None)) for key in left),
                 ((key, (None, dict2[key])) for key in right),
                 ((key, (dict1[key], dict2[key])) for key in intersection))


def merge_info(incoming, present):
    """ Merges incoming information with the present information """
    def custom_max(values):
        """
        Extracts the most recent information from the output of join_dicts.
        """
        (left, right) = values
        
        if right is None:
            return left
        elif left is None:
            return right
        else:
            return max(values, lambda x: x[0])
        
    return {key: custom_max(val) for key, val in join_dicts(incoming, present)}
    
def encode_info(local_info, remote_info):
    """ Encodes a shard's information in a format suitable for transmission """
    
    
