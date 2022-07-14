"""
Tests the shard module's Shard pseudo-class and ensures it can handle actors
"""

import asyncio
from inspect import getclosurevars
from typing import Callable, Awaitable

import pytest

from message_system_interface import MessageSystemInterface
from actor import Actor
from shard import Shard

class MockMessage(MessageSystemInterface):
    """ The tests will not use the messaging system """
    async def mailman(self, actor):
        pass
    async def send(self, address, message):
        pass
    
def MockFactory():
    """ Generates a fake message_system which does nothing. """
    return MockMessage()

def test_shard():
    loop = asyncio.get_event_loop()
    async def test():
        my_shard = Shard(Actor, MockFactory, "my_shard")
        
        @my_shard.actor("spawner")
        async def spawner(inst, message):
            """ Tries to get my_shard to spawn an actor """
            print(f"Spawner actor {inst.name} instantiated")
            @inst.action("go")
            async def go(message):
                """ tells my_shard to spawn another spawner """
                await my_shard.queue.put({"action": "spawn",
                                          "type": "spawner",
                                          })
            
        #Now for the actual tests. We begin by running my_shard
        
        await my_shard.run()
        
        #We then ensure there is a factory hidden in the closure.
        vars = getclosurevars(my_shard.actor)
        print(vars)
        print(vars.nonlocals)
        
        assert "factories" in vars.nonlocals
        assert len(vars.nonlocals["factories"]) == 1
        
        # Next we attempt to spawn an actor by forging a message
        await my_shard.queue.put({"action": "spawn", "type": "spawner"})
        await asyncio.sleep(0.5)  # Add some sleep just to be sure
        
        vars = getclosurevars(my_shard._actions["spawn"])
        print(vars.nonlocals)
        assert len(vars.nonlocals["actors"]) == 1

    loop.run_until_complete(test())
                