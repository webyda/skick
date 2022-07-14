import asyncio
import pytest

from simple_message import SimpleFactory, SimpleMessage

class MockActor:
    def __init__(self, name):
        self.name = name
        self.queue = asyncio.Queue()
        self.log = []
        
    async def run(self):
        while True:
            self.log.append(await self.queue.get())
            
        

def test_SimpleMessage():
    loop = asyncio.get_event_loop()
    async def test():
        actor = MockActor("1")
        actor2 = MockActor("2")
        ac_task = asyncio.create_task(actor.run())
        ac_task2 = asyncio.create_task(actor2.run())
        
        mf = SimpleFactory({"url": "123@example.com"})
        
        ms = mf.create()
        ms2 = mf.create()
        
        await ms.mailman(actor)
        
        assert "1" in mf.queues
        
        await ms.send("1", {"message"})
        await asyncio.sleep(.1)
        print(actor.log)
        assert actor.log[0] == {"message"}
        
        await ms2.mailman(actor2)
        
        await ms.send("2", {"message2"})
        await ms2.send("1", {"message3"})
        await asyncio.sleep(.1)
        print(actor.log)
        print(actor2.log)
        assert actor.log[1] == {"message3"}
        assert actor2.log[0] == {"message2"}
        
        ac_task.cancel()
        ac_task2.cancel()
    loop.run_until_complete(test())