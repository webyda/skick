"""
Contains a subclass of the Actor class specific for session actors. This type
of actor is paired with a FrontActor instance to create a subsession, that is a
syzygy of two actors that handle a websocket connection together.
"""

import asyncio
from secrets import token_hex

from .actor import Actor


class SocketQuery:
    """
    A class for signaling to the syzygy that a conversation between the back
    actor and some other actor is halted because the back actor requests a
    reply from the websocket client before it can continue.
    """

    def __init__(self, message, subschema):
        self.message = message
        self.subschema = subschema
        self.cid = None  # When we yield a SocketQuery, the Conversation object adds this field.


class BackActor(Actor):
    """
    This class is a companion class to the FrontActor class. It is intended to
    receive those messages from the websocket that the FrontActor has verified
    as schema compliant. It then performs some action on them. Apart from this
    it has helper methods for communicating with the websocket via the FrontActor.
    """

    def __init__(self, name, message_system=None, shard=None, loop=None):
        super().__init__(name, message_system=message_system, shard=shard, loop=loop)
        self._replacement_factories = {}  # A dictionary used for serializing
        self.associate = None
        self._socket_queries = {}

    def socket(self, name, schema):
        """
        This decorator is a companion to the socket decorator in the FrontActor.
        When the session factory function is run on the BackActor, it ignores the
        schema, but records the function as a standard action. This allows the
        FrontActor to send it approved messages from the websocket, acting only
        on those that are schema compliant.
        """
        return self.action(name)

    def _default_actions(self):
        """Adds an action for receiving SocketQuery replies"""
        self.action("query_reply")(self._sock_query_handler)
        super()._default_actions()

    async def _sock_query_handler(self, message):
        """
        Handles replies to a conversation from a socket to fill a request by
        injecting them in the normal conversation handler as if they came from
        """
        if message["query_id"] in self._socket_queries:
            qid = message["query_id"]
            cid = self._socket_queries[qid]
            msg = message["message"]
            del self._socket_queries
            await self._process_reply(
                {"action": "receive_reply", "cid": cid, "message": msg}
            )
        else:
            pass

    async def socksend(self, message):
        """Sends a message to the websocket via the front actor"""
        await self.send(self.associate, {"action": "socksend", "message": message})

    async def _replace_cleanse(self):
        """
        We wish to add a query clearing step in the replacement process. This
        will take place within the locked part of the process.
        """
        self._socket_queries = {}

    async def _replace_populate(self, factory, message):
        """
        We need to send a message to the front actor to tell it to replace its
        schemas. This needs to happen before the on_start method is run, so
        it is entirely appropriate to do it here. Due to the lock, the last
        replacement message sent corresponds to the last replacement operation.
        RabbitMQ also ensures messages will be delivered in order. Therefore,
        we will always have a consistent state eventually.

        Nota bene: We have previously sent the message to the front actor after
        the replacement operation. This causes a bug since the back actor may
        run another replacement operation in the on_start method, causing
        chains of replacements where the last one will take effect on the
        back actor, and the first one on the front actor. Therefore the order of
        operations is essential.
        """
        await self.send(
            self.associate,
            {"action": "replace_from_message", "factory": factory, "message": message},
        )

    async def replace(self, factory, message):
        """
        The replacement method needs to be adapted to websocket syzygies.
        Replacement always happens in pairs, where the session actor alters
        its behavior first, and then requests the receptionist actor to change
        its schema specifications.

        Nota bene: Here the order of operations matters. The replacement
        message must be sent to the front actor before we perform the
        replacement locally. If we do not do this, then the back actor can
        perform a second replacement within the super().replace() call
        because of the @on_start decorator. If this happens, then the back
        actor replacements will take immediate effect (because they are
        directly applied) but the front actor replacements will have to wait.
        Additionally, the front actor replacements will arrive in the reverse
        order, and the final front actor will not match the back actor. This
        bug has occurred in the past.
        """
        await super().replace(
            self._replacement_factories[factory],
            message,
            injected_populate=self._replace_populate(factory, message),
            injected_cleanse=self._replace_cleanse(),
        )

    async def _create_query(self, cid, query):
        query_id = token_hex()
        self._socket_queries[query_id] = cid
        await self.send(
            self.associate,
            {
                "action": "new_query",
                "message": query.message,
                "query_id": query_id,
                "schema": query.subschema,
            },
        )

    async def _wrap_response(self, reply, conversation):
        if isinstance(reply, SocketQuery):
            await self._create_query(conversation.cid, reply)
        else:
            await super()._wrap_response(reply, conversation)

    async def _reply_parser(self, reply):
        """
        This is a slightly ugly hack to allow the session actor to intercept
        queries meant for the websocket client, allowing it to set up a
        pipeline for sending the query to the client and receiving it back
        before the conversation resumes.

        The schema is presumed to be presupplied in the receptionist through
        a separate function. It will therefore have a fixed set of templates
        for response schemas. Any messages which do not conform to these
        subschemas will simply be rejected.
        """
        if isinstance(reply, SocketQuery):
            self._create_query(reply.cid, reply)
        else:
            await super()._reply_parser(reply)

    def front_action(self, name, schema=None):
        """
        Ignore actions meant for the receptionist actor
        """

        def decorator(func):
            return func

        return decorator

    def front_daemon(self, name):
        """Ignores daemons specifically meant to run on the front actor"""

        def decorator(func):
            return func

        return decorator

    def front_on_start(self, func):
        """
        Ignore start actions meant for the receptionist actor
        """
        return func

    def front_on_stop(self, func):
        """
        Ignore stop actions meant for the receptionist actor
        """
        return func

    async def run(self):
        """
        Starts the actor, adding a sentinel for the receptionist actor on top
        of the regular startup function.
        """
        await super().run()
