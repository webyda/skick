import asyncio
from gc import get_referrers
import pytest
from actor import Actor


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

        async def first_state(inst, message):
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

        async def second_state(inst, message):
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

        await first_state(actor, {})
        await actor.run()
        await asyncio.sleep(1)
        assert len(get_referrers(first_about)) == 1
        assert len(get_referrers(second_about)) > 1
        assert log[0][1]["inst.name"] == log[1][1]["inst.name"]
        assert log[0][1]["id(inst)"] == log[1][1]["id(inst)"]
        assert log[0][1]["inst.type"] == "first"
        assert log[1][1]["inst.type"] == "second"

    loop.run_until_complete(test())
