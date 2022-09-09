"""
This module contains a simple implementation of a distributed hash table meant
for single core systems. It relies directly on python dictionaries and
functions without the need for external servers and libraries.
"""

from .abstract_hash import AbstractHash

class SimpleHash(AbstractHash):
    dictionaries = {}
    def __init__(self, name):
        super().__init__()
        self.name = name
        if name not in self.dictionaries:
            self.dictionaries[name] = {}
        else:
            pass

    async def get(self, key):
        return self.dictionaries[self.name][key]

    async def set(self, key, value):
        self.dictionaries[self.name][key] = value

    async def delete(self, key):
        del self.dictionaries[self.name][key]

    async def has_key(self, key):
        """
        The asynchronous nature of the interface precludes using the normal
        "in" operator, so we have to implement it through a function.
        """
        return key in self.dictionaries[self.name]
