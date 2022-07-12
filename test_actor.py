import asyncio
from gc import get_referrers
import pytest
import actor




def actor_generator(id, messages):
    log = []
    class MockActor(actor.AbstractActor):
        """ A mock actor implementation for unit testing. """
        async def mailman(self):
            """ Creates a mock mailman that just puts messages into the queue """
            for msg in messages:
                print(msg)
                await self.queue.put(msg)
        
        async def send(self, address, message):
            """
            A mock send function that just actually just puts the message in a
            set.
            """
            nonlocal log
            print((address, message))
            log.append((address, message))
            
            return True
    
    return MockActor(id), log, MockActor


def test_actor():
    loop = asyncio.get_event_loop()
    async def test():
        actor, log, _= actor_generator("1", [{"action": "ping", "sender": "me", "payload": {}}])
        
        @actor.action("ping")
        async def pinger(message):
            print("Sending reply")
            await actor.send(message["sender"], {"action": "reply"})
        
        await actor.run()
        await asyncio.sleep(.5)
        assert log == [("me", {"action": "reply"})] 
        
        await actor.stop()
    loop.run_until_complete(test())
    
    
def test_replacement():
    loop = asyncio.get_event_loop()
    async def test():
        __, log, Actor = actor_generator("1", [{"action": "about"}, {"action": "replace"}, {"action": "about"}])
        
        first_about = None
        second_about = None
        
        async def first_state(inst, message):
            @inst.action("about")
            async def about(message):
                nonlocal inst
                nonlocal first_about
                first_about = inst._actions["about"]
                await inst.send("pytest", {"inst.name": inst.name, "id(inst)": id(inst), "inst.type": "first"})
                print(f"Name: {inst.name}")
                print(f"Actions: {inst._actions}")
            
            @inst.action("replace")
            async def replace(message):
                nonlocal inst
                await inst.replace(second_state, message)
                
        async def second_state(inst, message):
            @inst.action("about")
            async def about(message):
                nonlocal inst
                nonlocal second_about
                second_about = inst._actions["about"]
                await inst.send("pytest", {"inst.name": inst.name, "id(inst)": id(inst), "inst.type": "second"})
                print(f"Name: {inst.name}")
                print(f"Actions: {inst._actions}")
            
            @inst.action("replace")
            async def replace(message):
                nonlocal inst
                await inst.replace(first_state, message)
        
        ac = Actor("2")
        
        await first_state(ac, {})
        await ac.run()
        await asyncio.sleep(1)
        assert len(get_referrers(first_about)) == 1
        assert len(get_referrers(second_about)) > 1
        assert log[0][1]["inst.name"] == log[1][1]["inst.name"]
        assert log[0][1]["id(inst)"] == log[1][1]["id(inst)"]
        assert log[0][1]["inst.type"] == "first"
        assert log[1][1]["inst.type"] == "second"

    loop.run_until_complete(test())
        