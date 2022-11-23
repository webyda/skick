"""
This file provides a simple implementation of the hash interface using Redis.
"""

import asyncio
import redis.asyncio as redis

from .abstract_hash import AbstractHash


def prohibit_keys(func):
    """
    Raises an exception if a forbidden character is used in the key.
    For Redis, the following are banned:

    1. "{" and "}", which can be used by a malicious user to redirect data to
       a specific redis shard of their choosing, enabling them to perform a
       denial of service attack under some circumstances.
    2. The character ":" which allows a user, in theory, to circumvent the
       normal table selection mechanism, potentially allowing them to perform
       operations like SETs and GETs on unintended tables.
    """
    forbidden_characters = "{}:"

    async def decorated(self, key, *args, **kwargs):
        if any((character in key for character in forbidden_characters)):
            raise ValueError("Key contains a forbidden character.")
        else:
            return await func(self, key, *args, **kwargs)

    return decorated


class RedisHash(AbstractHash):
    """Contains a rudimentary Redis interface"""

    redis = None

    def __init__(self, name, *args, **kwargs):
        super().__init__()
        self.name = name

    @classmethod
    async def set_pool(cls, new_redis):
        """
        Sets the shard wide redis pool.
        """
        if isinstance(new_redis, str):
            cls.redis = await redis.from_url(new_redis)
        else:
            cls.redis = new_redis

    @prohibit_keys
    async def get(self, key):
        """Gets a key from redis"""
        return (await self.redis.get(f"{self.name}:{key}")).decode()

    @prohibit_keys
    async def set(self, key, val):
        """Sets a key in redis"""
        return await self.redis.set(f"{self.name}:{key}", val)

    @prohibit_keys
    async def delete(self, key):
        """Deletes a key from redis"""
        return await self.redis.delete(f"{self.name}:{key}")

    @prohibit_keys
    async def has_key(self, key):
        """Checks whether a given key exists in the table."""
        return (await self.redis.exists(f"{self.name}:{key}")) > 0
