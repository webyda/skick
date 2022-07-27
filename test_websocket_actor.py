"""
This module contains tests for the websocket_actor module.
"""

import asyncio
import json
from gc import get_referrers

import pytest

from websocket_interface import WebsocketConnectionObject, WebsocketServerInterface
from simple_message import SimpleFactory  # Almost a mock version
from websocket_actor import WebsocketActor


class MockSocket(WebsocketConnectionObject):
    """
    A mock socket that "receives" messages through a queue and "sends" them
    by appending them to a log.
    """
    def __init__(self):
        self._messages = asyncio.Queue()
        self._log = []
        self.closed = False

    async def recv(self):
        return await self._messages.get()

    async def send(self, message):
        self._log.append(message)

    async def close(self):
        self.closed = True
        await self._messages.put({})

    async def ping(self):
        """ Pings the client """
        self._log.append("ping")

    async def __anext__(self):
        """ Simply consumes the internal message queue """
        ret = await self._messages.get()  # This is necessary
        if self.closed and self._messages.empty():
            raise StopAsyncIteration
        else:
            return ret
    
    def __aiter__(self):
        """ Iterates over messages in the queue. """
        self.closed = False
        return self


class MockServer(WebsocketServerInterface):
    """
    A mock Websockets server implementation that starts false client
    connections with a prescribed interval to which false messages are sent
    as described in the constructor, and which logs all messages sent to the
    false clients instead of sending them over an actual socket.
    """
    def __init__(self, clients, client_interval=.1, message_interval=.13):
        self.handler = None
        self.clients = clients
        self.logs = []
        self.client_interval = client_interval
        self.message_interval = message_interval
        self.tasks = []

    async def serve(self, handler):
        def make_client(messages):
            sock = MockSocket()
            self.logs.append(sock._log)

            async def transmitter():
                for message in messages:
                    await asyncio.sleep(self.message_interval)
                    await sock._messages.put(json.dumps(message))
                await sock.close()
            return sock, transmitter

        async def master_task():
            for client in self.clients:
                await asyncio.sleep(self.client_interval)
                (sock, transmitter) = make_client(client)
                self.tasks.append(asyncio.create_task(transmitter()))
                self.tasks.append(asyncio.create_task(handler(sock)))

        self.tasks.append(asyncio.create_task(master_task()))

        return self

    def close(self):
        """ cancel any remaining subtasks """
        for task in self.tasks:
            task.cancel()


def test_websocket_actor():
    """
    Tests wheter we can construct a wokring syzygy through our WebsocketActor
    function.
    """
    loop = asyncio.get_event_loop()

    async def test():
        srv = MockServer([[{"session_type": "guest"},
                           {"action": "set_name", "name": "adrian"},
                           {"action": "get_name"}],
                          [{"session_type": "guest"},
                           {"action": "set_name", "name": "brian"},
                          {"action": "get_name"}]
                          ])

        msg = SimpleFactory({})
        wserver = WebsocketActor(srv, msg.create)

        async def queue_sniper():
            nonlocal msg
            nonlocal srv
            while(True):
                await asyncio.sleep(.5)
                print("Logs")
                print(srv.logs)
        qsnip = asyncio.create_task(queue_sniper())

        @wserver.session("guest")
        def guest(inst, message):
            """ A guest session """
            name = "adolf"
            print(inst.name, type(inst))

            @inst.socket("set_name", {"name": str})
            async def set_name(message):
                nonlocal name
                name = message["name"]
                print(name)

            @inst.socket("get_name", {})
            async def get_name(message):
                print(f"Socket named {name} received message {message}, sending message {{'action': 'name', 'name': name}}")
                await inst.send(inst.associate,
                                {"action": "socksend", "name": name})

        await wserver.run()

        # wait until we have ascertained that both actors have
        # communicated over their websockets
        while (tries := 0) < 2*10:
            if all(srv.logs) and srv.logs:
                break
            else:
                await asyncio.sleep(.1)
                tries += 1

        assert json.dumps({'action': 'socksend', 'name': 'adrian'}) in srv.logs[0]
        assert json.dumps({'action': 'socksend', 'name': 'brian'}) in srv.logs[1]

        qsnip.cancel()
        await wserver.stop()
        srv.close()

    loop.run_until_complete(test())


def test_replacement():
    """
    Tests whether the syzygy replacement mechanism works as intended.
    """
    loop = asyncio.get_event_loop()
    obj = ["This is a test object"]

    async def test():
        # We begin by setting up a mock server, with a sequence of messages
        # that will result in a replacement of the syzygy.
        srv = MockServer([[{"session_type": "anon"},
                           {"action": "set_name", "name": "adrian"},
                           {"action": "get_name"},
                           ]])
        msg = SimpleFactory({})

        wserver = WebsocketActor(srv, msg.create)

        # We, unfortunately, have to start with the latter state
        @wserver.subsession("named")
        def named(inst, message):
            """ A named session, where we can inquire the current name """
            print("Running name factory")
            name = message["name"]
            @inst.socket("get_name", {})
            async def get_name(message):
                print("Getting name")
                await inst.send(inst.associate, {"action": "socksend", "name": name})

        @wserver.session("anon")
        def anon(inst, messsage):
            """ An anonymous connection that can be promoted to a named one """
            name = ""
            print("Running anon factory")
            old_closure = obj
            @inst.socket("set_name", {"name": str})
            async def set_name(message):
                nonlocal name
                name = message["name"]
                await inst.replace(named, {"name": name})

        await wserver.run()

        while (tries := 0) < 20:
            if all(srv.logs) and srv.logs:
                break
            else:
                await asyncio.sleep(.1)
                tries += 1

        print(srv.logs)
        assert json.dumps({'action': 'socksend', 'name': 'adrian'}) in srv.logs[0]
        assert len(get_referrers(obj)) == 1

        srv.close()
        await wserver.stop()

    loop.run_until_complete(test())
