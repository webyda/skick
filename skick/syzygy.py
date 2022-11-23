"""
Contains a helper class for keeping track of subsessions syzygies so that we
can close them in an orderly fashion and supervise them.
"""

import asyncio

from .rate_limiter import RateExceeded


class Syzygy:
    """
    A small class that collates and coordinates the FrontActor and BackActor
    instances that make up a websocket syzygy. It is intended to be used to
    coordinate stopping in websocket connections.

    When a connection is opened, the WebsocketActor creates two actors, one of
    which consumes from the websocket in a separate task. The Syzygy class
    helps the WebsocketActor keep track of whether the consumer task encounters
    any errors, or if it finishes. When it does, the WebsocketActor will close
    the actors in the syzygy.

    If invidual actors in the syzygy have their sentinels triggered, the
    syzygy will provide a handler to stop both actors and close the connection
    in an orderly fashion.
    """

    WebsocketException = None

    def __init__(self, receptionist, session_actor, consumer):
        self.receptionist = receptionist
        self.session_actor = session_actor
        self.consumer = consumer

        self.stopping = False

    @property
    def addresses(self):
        return (self.receptionist.name, self.session_actor.name)

    async def stop(self):
        """
        Stops the syzygy by stopping both actors and cancelling the consumer
        task.
        """
        if not self.stopping:
            self.stopping = True
            self.consumer.cancel()
            await self.receptionist.stop()
            await self.session_actor.stop()

    async def supervise(self):
        """Keeps a watchful eye on the consumer task"""
        try:
            await self.consumer
        except self.WebsocketException:
            # In this case there has been some sort of websocket related issue
            # At present, we don't do anything.
            pass

        except RateExceeded:
            # In this case, the client has exceeded the rate limit. At present,
            # don't do anything.
            pass
        except asyncio.CancelledError:
            # In this case the consumer has been cancelled voluntarily
            pass
        else:
            # In this case, the consumer has closed the connection voluntarily
            pass
        finally:
            await self.stop()

    async def sentinel_handler(self, message):
        """
        Used to process sentinel messages. At present, it merely stops both
        actors irregardless of the actual message.
        """
        await self.stop()
