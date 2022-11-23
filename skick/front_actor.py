"""
Contains a subclass of the Actor class responsible for consuming from a websocket connection.
The FrontActor class inspects incoming messages on the websocket and forwards those it deems
compliant with one of its approved schemas.
"""

import asyncio
from orjson import JSONDecodeError, dumps, loads
from schema import Schema, Const, Or

from .actor import Actor
from .rate_limiter import TokenBucket


class FrontActor(Actor):
    """
    A specialized actor class for websocket receptionist actors. These actors
    are instantiated by the special Websocket Actor whenever a new connection
    is opened by a client. They are responsible for filtering messages from the
    client and sending them to a corresponding session actor for processing.
    """

    def __init__(self, name, message_system=None, shard=None, loop=None):
        super().__init__(name, message_system=message_system, shard=shard, loop=loop)

        self._websocket = None
        self.associate = None
        self._socket_schemas = {}
        self._replacement_factories = {}  # To deserialize factories

        self._default_actions()
        self._socket_task = None

        self._query_schemas = {}
        self._queries = {}
        self._default_schemas()

        self._ws_limiter = TokenBucket()

    def _default_actions(self):
        """
        Adds default behaviors for accepting communications from the assoicated
        session actor.
        """
        super()._default_actions()
        super().action("socksend")(self.socksend)
        super().action("replace_from_message")(self.replace_from_message)
        super().action("new_query")(self._new_query)

    def _default_schemas(self):
        """
        Adds some default subschemas to the front actor for standard query
        types. This eliminates the need to define new queries for common types
        of messages and replies from the client, so as to reduce clutter in
        downstream applications.
        """
        self._socket_schemas["query_reply"] = Schema(
            {"action": Const("query_reply"), "query_id": str, "message": object}
        ).is_valid
        self.query_schema("default", Or(str, int, float, bool))
        self.query_schema("str", str)
        self.query_schema("int", int)
        self.query_schema("float", float)
        self.query_schema("bool", bool)
        self.query_schema("list", [str, int, float, bool])

    def query_schema(self, name, schema):
        """Registers a schema to be used for some SocketQuery."""
        self._query_schemas[name] = Schema(schema)

    async def _process_reply(self, message):
        """
        Processes a reply from the client to a specific query. There are a
        variety of things which have not yet been checked by the ordinary system.
        Specifically, we need to ensure the message portion of the reply is
        consistent with the provided subschema, and that there is an actual
        active query with the given ID.
        """
        if message["query_id"] in self._queries:
            subschema = self._queries[message["query_id"]]
            if subschema.is_valid(message["message"]):
                await self.send(
                    self.associate,
                    {
                        "action": "query_reply",
                        "query_id": message["query_id"],
                        "message": message["message"],
                    },
                )
                del self._queries[message["query_id"]]
            else:
                await self.socksend({"message": {"error": "Invalid subschema"}})
        else:
            pass
            # Ignore this error

    async def _new_query(self, message):
        """
        A message handler for setting up client queries for the websocket.
        """
        if message["schema"] in self._query_schemas:
            self._queries[message["query_id"]] = self._query_schemas[message["schema"]]
            await self.socksend(
                {
                    "message": {
                        "action": "SocketQuery",
                        "query_id": message["query_id"],
                        "message": message["message"],
                        "schema": message["schema"],
                    }
                }
            )
        else:
            self._queries[message["query_id"]] = self._query_schemas["default"]
            await self.socksend(
                {
                    "message": {
                        "action": "SocketQuery",
                        "query_id": message["query_id"],
                        "message": message["message"],
                        "schema": message["schema"],
                    }
                }
            )

    async def replace_from_message(self, message):
        """
        Receives a message from the associated session actor and performs
        the replacement therein specified.
        """
        factory = self._replacement_factories[message["factory"]]
        await self.replace(
            factory,
            message["message"],
            injected_cleanse=self._replace_cleanse(),
            injected_populate=self._replace_populate(),
        )

    async def _replace_cleanse(self):
        """
        Adds a step to the replacement process so that we can clear the
        websocket specific stuff.
        """
        self._socket_schemas.clear()
        self._query_schemas.clear()

    async def _replace_populate(self):
        """
        Adds a step to the replacement process so that we can populate the
        websocket specific dictionaries.
        """
        self._default_schemas()

    def on_start(self, func):
        """
        We wish to ignore on_start and on_stop reserving them for the back
        actor.
        """
        return func

    def on_stop(self, func):
        """
        We wish to ignore on_start and on_stop reserving them for the back
        actor.
        """
        return func

    def front_on_start(self, func):
        """
        Allow the receptionist to be assigned startup actions.
        """
        return super().on_start(func)

    def front_on_stop(self, func):
        """
        Allows the receptionist to be assigned stop actions.
        """
        return super().on_stop(func)

    def action(self, name, schema=None):
        """
        We do not wish to register actions by default in front actors.
        This is because we would prefer if "socket" methods were set up
        as syzygy actions, while "action" methods refer to the session actor
        and allows it to interact with ordinary actors.
        """

        def decorator(func):
            return func

        return decorator

    def daemon(self, name):
        """
        Prevent daemons from running on the front actor by default
        """

        def decorator(func):
            return func

        return decorator

    def front_daemon(self, name):
        """
        Allows running a daemon specifically on the front actor.
        """
        return super().daemon(name)

    def front_action(self, name, schema=None):
        """
        In some cases, we may be interested in specifically assigning an action
        to the receptionist actor. In these rare cases, we can invoke this
        method to specify that we wish to assign an action to the receptionist.
        """
        return super().action(name)

    def socket(self, name, schema):
        """
        This decorator acts a bit like the action decorator for normal actors.
        The difference is that it defines a schema such that if a message arrives
        on the websocket that conforms to the schema, then the message is sent
        to the session actor which processes the message in accordance with the
        provided method.

        In the FrontActor, this merely records the schema.
        """

        def decorator(func):
            validator = Schema({**schema, **{"action": Const(name)}}).is_valid
            self._socket_schemas[name] = validator
            return func

        return decorator

    async def socksend(self, message):
        """Sends a message over the websocket if there is one."""
        if self._websocket:
            await self._websocket.send(dumps(message["message"]).decode())

    async def socklistener(self):
        """Listens to the websocket"""
        async for message in self._websocket:
            await self._ws_limiter.waiter()

            try:
                message = loads(message)
            except JSONDecodeError:
                await self.socksend(
                    {"message": {"action": "error", "type": "invalid JSON"}}
                )
            else:

                try:
                    action = message.get("action")
                except AttributeError:
                    await self.socksend(
                        {
                            "message": {
                                "action": "error",
                                "type": "Incorrect message format",
                            }
                        }
                    )
                    continue

                if action:
                    if action in self._socket_schemas:
                        valid = self._socket_schemas[action](message)
                        if valid:
                            if not action == "query_reply":
                                await self.send(self.associate, message)
                            else:
                                await self._process_reply(message)
                        else:
                            await self.socksend(
                                {
                                    "message": {
                                        "action": "error",
                                        "type": "not schema compliant",
                                    }
                                }
                            )
                    else:
                        await self.socksend(
                            {
                                "message": {
                                    "action": "error",
                                    "type": f"No schema present for action '{action}'",
                                }
                            }
                        )
                else:
                    await self.socksend(
                        {"message": {"action": "error", "type": "No action requested"}}
                    )

    async def run(self):
        """Starts the actor and starts listening to the websocket."""
        await super().run()
        self._socket_task = self.loop.create_task(self.socklistener())

    async def _stop(self):
        """Stops the websocket listener"""
        if self._socket_task:
            self._socket_task.cancel()
        await super()._stop()
