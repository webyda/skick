from abc import ABC, abstractmethod
from typing import Callable, Awaitable


class WebsocketConnectionObject(ABC):
    """
    An abstract class for handling an individual connection between a
    particular client and the server. This class mimics the interface from the
    Websockets library, but we wish to have some superficial separation just
    in case another library shows up in the future.
    """

    @abstractmethod
    async def recv(self):
        """
        Awaits the arrival of a message from the client.
        """
        pass

    @abstractmethod
    async def send(self, message):
        """
        Sends a message to the client.
        """
        pass

    @abstractmethod
    async def close(self):
        """Closes the connection"""

    @abstractmethod
    async def ping(self):
        """Pings the client"""

    @abstractmethod
    def __aiter__(self):
        """
        Returns an asynchronous iterator which yields messages from the client.
        """
        pass

    @abstractmethod
    async def __anext__(self):
        """
        The actual method that feeds the next message from the client.
        """
        pass


class WebsocketServerInterface(ABC):
    """
    This is an interface which abstracts away the websocket library so as to
    separate concerns and allow for easy testing.

    A websocket server interface needs to be able to:
    1. Accept new connections,
    2. Handle these connections by invoking a provided handler and spawning
       tasks,
    3. Maintain present connections.
    """

    @abstractmethod
    async def serve(
        self, handler: Callable[[WebsocketConnectionObject], Awaitable[None]]
    ) -> None:
        """
        Start the websocket server. The handler is a callable which must be
        given recv, send, close and ping functions, as well as __aiter__,
        and __anext__.

        This just happens to be exactly the protocol implemented by the
        Websockets library, but we wish to keep it classy just in case another
        library shows up on the block.
        """
        pass

    @classmethod
    @abstractmethod
    def get_exception_class(cls):
        """
        Returns the exception class that the websocket library uses.
        """
        pass
