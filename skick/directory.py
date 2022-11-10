"""
Contains a *directory* class the purpose of which is to transfer factory
functions between python modules. This way, we do not have to define all the
actors in the same module as the skick instance, but can import them from other
submodules. This is similar to blueprints in Sanic and Flask.
"""


class Directory:
    """
    Absorbs sessions, subsessions, actors and handshakes into dictionaries
    that can then be used to construct a skick instance, optionally registering
    a path prefix.
    """

    def __init__(self, prefix=None):
        self.prefix = prefix or ""
        self.sessions = {}
        self.subsessions = {}
        self.actors = {}

    def session(self, name):
        """
        Decorator for registering a session factory function
        """

        def decorator(func):
            self.sessions[name] = func
            return func

        return decorator

    def subsession(self, name):
        """
        Decorator for registering a subsession factory function
        """

        def decorator(func):
            self.subsessions[name] = func
            return func

        return decorator

    def actor(self, name):
        """
        Decorator for registering an actor factory function
        """

        def decorator(func):
            self.actors[name] = func
            return func

        return decorator

    def handshake(self, func):
        """
        Creates a handshake decorator that creates a pseudo-handshake decorator
        """

        def outer_decorator(func):
            def parameter(name):
                def inner_decorator(func_inner):
                    self.sessions[name] = func
                    self.subsessions[f"{name}:final"] = func_inner
                    return func_inner

                return inner_decorator

            return parameter

        return outer_decorator(func)
