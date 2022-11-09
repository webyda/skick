"""
Skick, an actor framework for Python 3.8+
"""

from .skick import Skick
from .directory import Directory
from .conversation import Call, Respond
from .websocket_actor import SocketQuery
from .actor import Actor
from .addressing import get_address
from .cluster import cluster_data

__all__ = [
    "Skick", # A facade orchestrating the whole system
    "Directory", # An actor registry
    "Call", # A class used to initiate calls to other actors
    "Respond", # A class used to respond to calls from other actors
    "SocketQuery", # A class used to query a websocket client
    "Actor",
    "get_address",
    "cluster_data",
    ]

if __name__ == "__main__":
    print("Skick, an actor framework for Python 3.8+")
    print("")
    print("Skick is not intended to be run as a script. Please import it into one of your")
    print("projects instead, and instantiate a Skick object to get started.")
          