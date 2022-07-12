import asyncio
from abc import ABC, abstractmethod
from secrets import token_urlsafe

class AbstractActor(ABC):
    """
    An abstract base class for actors in the Skick library.
    
    It requires concrete implementations that deliver messages to the queue and
    that can dispatch messages to external actors.
    
    The actor is instantiated by the use of a factory function that
    declaratively adds a number of message handlers. The handlers, called
    actions, share a closure which contains the actual body of the actor. The
    actions may either alter the state of the closure, send a message to a
    different actor, create new actors, or perform a "replacement", that is,
    replacing the current actions and creating a new closure for the new
    actions.
    
    Replacement being a comparatively complicated and slow operation, the
    preferred method of state management is simply to directly alter the state
    of the inner closure (something which is not considered in Agha due to
    theoretical constraints). This is essentially equivalent to replacement, but
    is more convenient from the programmer's standpoint in many cases.
    """
    def __init__(self, name, queue_size=100):
        self.name = name
        self.queue = asyncio.Queue(queue_size)
        self.loop = asyncio.get_event_loop()
        self._actions = {}
        
        self._message_task = None
        self._mailman_task = None
        
    async def _process_messages(self):
        """
        Consumes incoming messages from the internal queue and activates the
        appropriate handler.
        """
        while True:
            message = await self.queue.get()
            if message["action"] in self._actions:
                await self._actions[message["action"]](message)
            else:
                pass

    
    def action(self, name):
        """ Adds a regular action to the actor. """
        def decorator(func):
            self._actions[name] = func
            return func
        return decorator

    
    async def replace(self, factory, message):
        """
        Replaces the current actor's behaviors with another. Also replaces
        the state encapsulating closure. It does this by running the factory
        function of the new actor after having cleared the _actions dict.
        """
        self._actions.clear()
        await factory(self, message)
    
    
    @abstractmethod
    async def mailman(self):
        """
        This method is meant to attach some method of receiving messages to the
        actor. Concrete subclasses must implement this so that it receives
        messages using some sort of concrete mechanism like RabbitMQ, Kafka,
        or some other message queue, and then delivering them to the actor's
        internal queue. This serparates concerns and inverts dependencies.
        """
        pass


    @abstractmethod
    async def send(self, address, message):
        """
        This method allows the actor to send messages to other actors. It must
        be adapted to some concrete message propagation mechanism to work, and
        this mechanism may depend on the shard, the recipient and all manner of
        other things.
        """
        pass


    async def run(self):
        """ Runs the tasks associated with the actor """
        await self.mailman()
        
        self._message_task = self.loop.create_task(self._process_messages())


    async def stop(self):
        """ Kills the actor and cleans up """
        if self._mailman_task:
            self._mailman_task.cancel()
        
        self._message_task.cancel()