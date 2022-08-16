"""
Contains unit tests for the Actor class
"""

import asyncio
from gc import get_referrers
import pytest
from actor import Actor
from shard import Shard
from simple_message import SimpleFactory
from conversation import Call, Respond
class MockMSI:
    """ A mock Message System """
    def __init__(self, logger, messages):
        self.logger = logger
        self.messages = messages

    async def send(self, address, message):
        """ A mock send method tat actually just logs and prints messages """
        print(f"Sending [{message}] to [{address}]")
        if self.logger:
            self.logger((address, message))

    async def mailman(self, actor):
        """
        A mock mailman that just takes the provided messages and puts them in
        the queue.
        """
        for msg in self.messages:
            await actor.queue.put(msg)


def test_actor():
    """ Tests the actor class' ability to handle received messages """
    loop = asyncio.get_event_loop()

    async def test():
        log = []

        def logger(item):
            log.append(item)

        actor = Actor("1", message_system=MockMSI(logger, [{"action": "ping",
                                                            "sender": "me",
                                                            "payload": {},
                                                            }]))

        @actor.action("ping")
        async def pinger(message):
            print("Sending reply")
            await actor.send("me", {"action": "reply"})

        await actor.run()
        await asyncio.sleep(.5)
        assert log == [("me", {"action": "reply"})]

        await actor.stop()
    loop.run_until_complete(test())


def test_replacement():
    """ Tests actors' replacement actions """
    loop = asyncio.get_event_loop()

    async def test():
        log = []

        def logger(item):
            log.append(item)

        actor = Actor("1", MockMSI(logger, [{"action": "about"},
                                            {"action": "replace"},
                                            {"action": "about"},
                                            ]))

        first_about = None
        second_about = None

        def first_state(inst, message):
            @inst.action("about")
            async def about(message):
                nonlocal inst
                nonlocal first_about
                first_about = inst._actions["about"]
                await inst.send("pytest", {"inst.name": inst.name,
                                           "id(inst)": id(inst),
                                           "inst.type": "first",
                                           })
                print(f"Name: {inst.name}")
                print(f"Actions: {inst._actions}")

            @inst.action("replace")
            async def replace(message):
                nonlocal inst
                await inst.replace(second_state, message)

        def second_state(inst, message):
            @inst.action("about")
            async def about(message):
                nonlocal inst
                nonlocal second_about
                second_about = inst._actions["about"]
                await inst.send("pytest", {"inst.name": inst.name,
                                           "id(inst)": id(inst),
                                           "inst.type": "second",
                                           })
                print(f"Name: {inst.name}")
                print(f"Actions: {inst._actions}")

            @inst.action("replace")
            async def replace(message):
                nonlocal inst
                await inst.replace(first_state, message)

        first_state(actor, {})
        await actor.run()
        await asyncio.sleep(1)
        assert len(get_referrers(first_about)) == 1
        assert len(get_referrers(second_about)) > 1
        assert log[0][1]["inst.name"] == log[1][1]["inst.name"]
        assert log[0][1]["id(inst)"] == log[1][1]["id(inst)"]
        assert log[0][1]["inst.type"] == "first"
        assert log[1][1]["inst.type"] == "second"

    loop.run_until_complete(test())



def test_conversation():
    """
    This is a test which is designed to test the conversation functionality
    of actors. It simply attempts to see if the handsake, reply and close
    mechanics work vaguely as expected. Due to the nature of the mechanism
    being tested, it is not realistic to make a proper unit test. Instead, we
    will have to perform something closer to an integration test.
    """
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loga = []
    logb = []
    msg = SimpleFactory({})
    shard = Shard(Actor, msg.create, shard_id = "shard", loop = loop)
    
    @shard.actor("A")
    def A(inst, message):
        """ An actor that can converse with other versions of itself """
        print("A Instantiated")
        async def on_close():
            print("A closing")
            loga.append("CLOSED")
            
        @inst.conversation("call_other")
        async def call(message):
            print("A initiating conversation")
            resp = yield Call("B",
                       {"action": "receive_call",
                        "message": "Hello, I am A"},
                       on_close=on_close)
            print(f"A received reply {resp}")
            loga.append(resp)
            if resp == "Hello, I am B":
                resp = yield "Hi B, nice to meet you"
            else:
                resp = yield "I only talk to B. Goodbye"
            
            loga.append(resp)
            
    
    @shard.actor("B")
    def B(inst, message):
        print("B initiated")
        async def on_close():
            logb.append("CLOSED")
        
        @inst.conversation("receive_call")
        async def pick_up(message):
            """
            This indicates to B that A is initiating a conversation.
            In such cases B expects A to make the first move.
            """
            print(f"B responding to conversation beginning with {message}")
            yield Respond(message, on_close=on_close)
            print("B getting ready to respond")
            logb.append(message)
            
            resp = yield "Hello, I am B"
            print("Got here")
            logb.append(resp)
    
    async def test():
        await shard.run()
        await shard.send("shard",{"action": "spawn", "type": "A", "name": "A"})
        await shard.send("shard",{"action": "spawn", "type": "B", "name": "B"})
        
        await asyncio.sleep(.1)
        
        await shard.send("A", {"action": "call_other"})
        
        await asyncio.sleep(.1)
    
    loop.run_until_complete(test())
    
    print(loga)
    assert loga == ["Hello, I am B", "CLOSED"]
    print(logb)
    assert logb[0]["action"] == "receive_call"
    assert logb[0]["message"] == "Hello, I am A"
    assert logb[1:] == ["Hi B, nice to meet you", "CLOSED"]
    
        
        
        
        