"""
Provides an implementation of the websocket interface in the Websockets
library. This, in a sense, is the reference implementation of the interface,
around which it was designed, Websockets being the most commonly used websocket
library for Python. In this sense it is a "plain vanilla" implementation.
"""
from websockets import server
from websocket_interface import WebsocketConnectionObject, WebsocketServerInterface


# We mock a connection object class to provide the downstream programmer with
# The Full Experience (tm) of a custom connection object.
WebsocketConnectionObject.register(server.WebSocketServerProtocol)
PlainConnectionObject = WebsocketConnectionObject


class PlainWebsocket:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.server_object = None
        
    async def serve(self, handler):
        """
        Mimics the websockets.serve function.
        """
        self.server_object = await server.serve(handler, *self.args, **self.kwargs)
        return self.server_object
    
        
    