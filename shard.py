"""
Contains a function that generates a shard actor. This is a factory function
that takes a concrete actor type and attaches the sufficient behaviors for it
to manage spawning, deletion and distribution of new actors over a cluster.

The instances of the shard actor also serve as decorators with which we
register child actor factories and associate them with message templates.
"""

import asyncio
from secrets import token_urlsafe
from typing import Callable, Type, Awaitable
from message_system_interface import MessageSystemInterface
from actor import Actor

def Shard(actor_base: Type[Actor],
          message_factory: Callable[[], Awaitable[MessageSystemInterface]],
          shard_id: str=None) -> Actor:
    """
    Takes a concrete actor type and returns an actor
    """
    shard_actor = actor_base(shard_id if shard_id else token_urlsafe(16), message_factory())

    factories = {}  # This keeps track of all available factory functions
    actors = {}  # This keeps track of all managed actor instances

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
