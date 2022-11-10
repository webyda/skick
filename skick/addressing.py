"""
This module contains a pseudo-singleton for generating addresses for shards
and actors. The addresses it generates have the form shard:actor.

This is meant to be employed in message systems. Previously, it was found that
the overhead of creating separate RabbitMQ queues for each new actor was unacceptably
high. Instead, we would like to use one queue per shard and then use more
efficient shard-local messaging to route the message to the proper actor.
In order to achieve this, we would like to provide a routing hint in the address
so that the message system can easily know where to send its messages.
"""

from secrets import token_hex


SHARD = None


def get_address(shard=None, is_shard=False):
    """
    Returns a string of the form shard:actor. If is_shard is True,
    it returns shard:shard and sets the shard variable.

    If no shard is provided, it will use a global "SHARD" variable which is
    set on a per shard basis when get_address is called with the is_shard flag
    set.
    """
    global SHARD
    if is_shard:
        SHARD = token_hex()
        return f"{SHARD}:{SHARD}"
    else:
        if shard:
            return f"{shard[:len(shard)//2]}:{token_hex()}"
        elif SHARD:
            return f"{SHARD}:{token_hex()}"
        else:
            raise ValueError("Shard not set")
