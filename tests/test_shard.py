"""
Tests the shard module's Shard pseudo-class and ensures it can handle actors
"""

import asyncio
from inspect import getclosurevars

import pytest

from message_system_interface import MessageSystemInterface
from actor import Actor
from shard import Shard, encode, merge


class MockMessage(MessageSystemInterface):
    """ The tests will not use the messaging system """
    def __init__(self, message_log=None):
        print(f"Initiating MockMessage with message_log = {message_log}")
        if message_log is not None:
            self.message_log = message_log
        else:
            self.message_log = None
        print(f"self.message_log set to {self.message_log}")
    async def mailman(self, actor):
        pass

    async def send(self, address, message):
        print("Send request issued")
        print(f"(self._message_log = {self.message_log}) is {'' if self.message_log is None else 'not '} None")
        if self.message_log is not None:
            print(f"Adding message {[address, message]} to message log")
            self.message_log.append([address, message])
        else:
            pass

    async def broadcast(self, message):
        print("Broadcast request issues")
        print(f"Message to broadcast: {message}")
        if self.message_log is not None:
            print(f"Adding message {message} to message log")
            self.message_log.append(["broadcast", message])

    async def register_shard(self, address):
        pass

def MockFactory(message_log = None):
    """ Generates a fake message_system which does nothing. """
    def MockFactoryMocker():
        return MockMessage(message_log=message_log)
    return MockFactoryMocker

def test_encode():
    ret = encode("shard",
                 {"ironing": {}},
                 {"ironing": {"heros": {"ironman", "laundryman"}}},
                 {"ironing": {}},
                 {"heros": {"ironman", "laundryman"}},
                 {})
    print(ret)
    assert ret
    
def test_merge():
    A = {"apa": {"Herr Nilsson", "Nicke Nyfiken"},
         "häst": {"Lilla Gubben"},
         }
    B = {"apa": {"Herr Nilsson", "Godzilla"},
         "katt": {"Pelle Svanslös", "Elake Måns"},
         }
    
    out = merge(A, B, "apa")
    
    assert out == {"Herr Nilsson", "Godzilla", "Nicke Nyfiken"}
    
def test_shard():
    """ Tests whether we can instantiate a shard and use it """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def test():
        my_shard = Shard(Actor, MockFactory(), "my_shard", loop=loop)

        @my_shard.actor("spawner")
        def spawner(inst, message):
            """ Tries to get my_shard to spawn an actor """
            print(f"Spawner actor {inst.name} instantiated")

            @inst.action("go")
            async def go(message):
                """ tells my_shard to spawn another spawner """
                await my_shard.queue.put({"action": "spawn",
                                          "type": "spawner",
                                          })

        # Now for the actual tests. We begin by running my_shard
        await my_shard.run()

        # We then ensure there is a factory hidden in the closure.
        clovars = getclosurevars(my_shard.actor)
        print(clovars)
        print(clovars.nonlocals)

        assert "factories" in clovars.nonlocals
        assert len(clovars.nonlocals["factories"]) == 1

        # Next we attempt to spawn an actor by forging a message
        await my_shard.queue.put({"action": "spawn", "type": "spawner"})
        await asyncio.sleep(0.5)  # Add some sleep just to be sure

        clovars = getclosurevars(my_shard._actions["spawn"])
        print(clovars.nonlocals)
        assert len(clovars.nonlocals["actors"]) == 1

    loop.run_until_complete(test())


def test_registry():
    """ Tests if we can register a service properly """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async def test():
        sh_log = []
        sh = Shard(Actor, MockFactory(message_log=sh_log), "shard", loop=loop)

        @sh.actor("printer")
        def printer(inst, message):
            """ An actor whose sole job is to print the messages it receives """
            @inst.action("print")
            async def pr_msg(message):
                print(message)


        await sh.run()
        print(sh._daemon_tasks)
        #Since we don't actually have a messaging system, we just add messages
        await sh.queue.put({"action": "spawn",
                            "type": "printer",
                            "name": "printer"})
        await sh.queue.put({"action": "register_service",
                            "service": "printer",
                            "address": "printer"})
        await sh.queue.put({"action": "request_service",
                            "service": "printer",
                            "sender": "test_registry"})
        await asyncio.sleep(1)

        print(sh_log)
        assert sh_log
        assert sh_log[0][1]["action"] == "service_delivery"
        assert sh_log[0][1]["service"] == "printer"
        assert "printer" in sh_log[0][1]["local"]

        await sh.stop()


    loop.run_until_complete(test())


def test_whisper_broadcast():
    """
    A test to see whether the whisper and broadcast functions work.
    A number of false shards are added to the registry, so that we may see
    whether the real shard performs its broadcasts and whispers.
    This test is time consuming since it is stochastic in nature.

    With 10 shards, the likelihood of a broadcast is .1 every second, so in
    order to get a 95 % likelihood of passing, we need to wait around
    30 seconds. In order to to get a 99 % likelihood of the broadcast firing,
    we need to wait around 45 seconds. In order to get a 99.9 % likelihood of
    having a broadcast firing on our shard, we need to wait 66 seconds.
    """
    DESIRED_CONFIDENCE = .95
    loop = asyncio.new_event_loop()

    async def test():
        mlog = []
        whisper_seen = False
        broadcast_seen = False
        sh = Shard(Actor, MockFactory(message_log = mlog), "shard", loop=loop)

        # Prepare a false broadcast to allow the shard to incorporate false
        # remote information, allowing it to whisper and broadcast properly.
        false_broadcast = {"action": "receive_info",
                           "services": {"ironing": ["ironman", "laundryhero"]},
                           "dead_services": {"ironing": ["housemaid"]},
                           "shards": {"superheros": ["ironman", "laundryhero"],
                                      **{str(i): [] for i in range(8)}},
                           "dead_shards": {"the_old_shard": ["housemaid"]},
                           }
        
        await sh.run()
        await sh.queue.put(false_broadcast)
        
        await sh.queue.put({"action": "request_service",
                                "service": "ironing",
                                "sender": "shard",})
        await asyncio.sleep(.1)
        assert mlog
        mlog.clear()
        i=0
        def is_whisper(message):
            (address, msg) = message    
            return not address == "broadcast" and msg["action"] == "receive_info"
        
        while ((not whisper_seen or not broadcast_seen)
               and (1-.9**i) < DESIRED_CONFIDENCE):
            await asyncio.sleep(1)
            whisper_seen = any(map(is_whisper, mlog))
            broadcast_seen = any(map(lambda x: x[0] == "broadcast", mlog))
            
            i+=1
        print(mlog)
        await sh.stop()
        assert whisper_seen
        assert broadcast_seen
    loop.run_until_complete(test())