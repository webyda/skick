"""
This module contains a simple implementation of a distributed hash table meant
for single core systems. It relies directly on python dictionaries and
functions without the need for external servers and libraries.
"""

from .abstract_hash import AbstractHash


class SimpleHash(AbstractHash):
    """
    Provides a simple dictionary based implementation of the AbstractHash
    interface. The purpose is to allow the user to write code that *would*
    work, were they to attach a hash table like redis to their Skick instance.
    This way the user can write code that will more or less scale to a cluster
    without any substantial code changes. This is a central goal of Skick.
    """

    dictionaries = {}

    def __init__(self, name):
        super().__init__()
        self.name = name
        if name not in self.dictionaries:
            self.dictionaries[name] = {}
        else:
            pass

    async def get(self, key):
        """Simply gets the value from the dictionary."""
        return self.dictionaries[self.name][key]

    async def set(self, key, value):
        """Simply sets the value in the dictionary."""
        self.dictionaries[self.name][key] = value

    async def delete(self, key):
        """Simply deletes the key from the dictionary."""
        del self.dictionaries[self.name][key]

    async def has_key(self, key):
        """
        Detect whether a key is present in the dictionary.
        """
        return key in self.dictionaries[self.name]
