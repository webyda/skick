"""
This module contains a basic interface for constructing distributed hash
tables as a native datatype. The idea is to let actors in a cluster share
a global hash table, so that the table will be not only similar (though not
at all times necessarily exactly the same) for all actors, and so that they 
can levrage the combined memory of several servers. This could be used for
things like storing user session ids and similar things.
"""
import asyncio

from abc import ABC, abstractmethod


class AbstractHash(ABC):
    """
    An abstract base class which defines a protocol for interacting with a
    distributed hash table system. Note that the hashtable is allowed to impose
    conditions on permissibility and impermissibility of keys and table names.
    This is because in important popular backends these may serve various
    functions. Fore example, In Redis, we may need to separate tables by using
    special characters (such as "user:awnns" to denote the key "awnns" in the
    "user" table), and this could feasibly be exploited by a malicious user.
    
    The class also provides asynchronous versions of the __getitem__,
    __setitem__ and __delitem__ methods through the use of tasks and futures.
    We can not use these dunders directly, because they do not allow
    asynchronous usage. If we were to use a synchronous version of the redis
    client, then any holdup would cause the entire shard to become unavailable.
    """
    loop = None # We store the event loop as a class variable
    
    def __init__(self):
        """
        In order to be able to conveniently use futures, we need to be able to
        store them until they are done. We will use a naughty trick, defying
        the documentation, employing add_done_callback to remove the tasks from
        the internal registry when they are done.
        """
        self.tasks = set()
        
    def task_callback(self, future):
        """
        Simply removes the future from self.tasks. If it is being awaited, a
        separate reference exists in application code so it will not be garbage
        collected.
        """
        self.tasks.discard(future)
        
    @abstractmethod
    async def get(self, key: str):
        """
        Retrieve the field associated with the key key.
        """
        pass
    
    @abstractmethod
    async def set(self, key: str, value: str):
        """
        Set the field associated with the key key to the value value.
        """
        pass
    
    @abstractmethod
    async def delete(self, key: str):
        """
        Delete a key from the hash table.
        """
        pass
    
    @abstractmethod
    async def has_key(self, key):
        """
        Due to the asynchronous nature of the system, we can not use the
        regular "in" operator, instead we have to devise a method that
        takes care of that function.
        """
        pass
    
    def __getitem__(self, key):
        """
        Uses the asynchronous method defined by the user to return a future
        in which the appropriate key has been fetched.
        """
        if self.loop:
            task = self.loop.create_task(self.get(key))
            self.tasks.add(task)
            task.add_done_callback(self.task_callback)
            
            return task
        else:
            pass
    
    def __setitem__(self, key, val):
        """
        Sets an item asynchronously using a future. Not that there is no way
        for the user to acquire a reference to the future in question. This
        means.
        """
        print(f"Setting {key} to {val} asynchronously")
        if self.loop:
            task = self.loop.create_task(self.set(key, val))
            self.tasks.add(task)
            task.add_done_callback(self.task_callback)
        else:
            pass
        
    def __delitem__(self, key):
        """
        Deletes an item from the dictionary asynchronously by scheduling a
        task.
        """
        if self.loop:
            task = self.loop.create_task(self.delete(key))
            self.tasks.add(task)
            task.add_done_callback(self.task_callback)
        else:
            pass

    