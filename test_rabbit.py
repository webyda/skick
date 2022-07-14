"""
Tests whether the rabbit_message module works as expected provided a
RabbitMQ server is running on localhost.
"""
import asyncio

import pytest

import rabbit_message

LOGIN = "amqp://guest:guest@localhost:5672/"


class MockActor:
    """ A mock actor containing the bare minimum for our tests. """
    def __init__(self, name):
        self.queue = asyncio.Queue()
        self.name = name


def test_rabbit_factory():
    """ Tests wheter the rabbit factory works """
    loop = asyncio.get_event_loop()

    async def test():
        factory = rabbit_message.RabbitFactory(LOGIN)
        assert factory.connection
        assert factory.channel
        factory.create()

    loop.run_until_complete(test())


def test_rabbit_message():
    """
    Tests whether messages are actually propagated by the RabbitMQ server
    through the interface that the Rabbit factory generates.
    """
    loop = asyncio.get_event_loop()

    async def test():
        factory = rabbit_message.RabbitFactory(LOGIN)
        print("Factory created")
        act1 = MockActor("act1")
        act2 = MockActor("act2")
        print("Actors instantiated")

        mgs1 = factory.create()
        mgs2 = factory.create()
        print("Factories ran")

        clean1 = await mgs1.mailman(act1)
        clean2 = await mgs2.mailman(act2)
        print("Mailmen ran")

        await mgs1.send("act2", {"msg": "henlo"})
        await mgs1.send("act2", {"msg2": "henlo2"})

        await mgs2.send("act1", {"msg": "henlo"})

        print("Messages sent")
        await asyncio.sleep(.1)

        assert await act1.queue.get() == {"msg": "henlo"}
        assert await act2.queue.get() == {"msg": "henlo"}
        assert await act2.queue.get() == {"msg2": "henlo2"}

        await clean1()
        await clean2()
    loop.run_until_complete(test())


if __name__ == "__main__":
    test_rabbit_message()
