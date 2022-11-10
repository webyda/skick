"""
This module contains a class for managing conversations between actors.
It is a rather thin, but keeps things nice and tidy.
"""
from secrets import token_hex


class Conversation:
    """
    A class for managing conversations so as to not pollute the actor class.
    It contains a generator, some helper methods and mechanisms for managing,
    closing and opening conversations in an appropriate and clean way.
    """

    def __init__(self, partner: str, cid: str = None, on_close=None):
        self.partner = partner
        self.name = None
        self.generator = None
        self.on_close = on_close
        self.greeting = None
        if not cid:
            self.cid = token_hex()
        else:
            self.cid = cid

    async def attach(self, generator, name: str):
        self.generator = generator
        self.name = name

    async def process_message(self, message: dict):
        """
        This method processes a message by sending it to the (hopefully)
        attached generator and awaiting its reply. If the generator raises
        StopAsyncIteration, the conversation is closed.

        The generator can respond in a few different ways.

        1. It can return a nonempty dict, in which case the dict is a reply
           to the partner.
        2. It can return a non-dict, True value, in which case the handling is
           deferred to the actor for further consideration. This is to allow
           an escape hatch from the conventional conversation system for things
           like websocket client queries mid-conversation.
        2. It can return a dict with an end_conversation action, in which case
           it has decided to terminate the conversation amicably.
        3. It may have raised a StopAsyncIteration exception, in which case
           it simply terminated by virtue of an implicit return. In this case,
           we catch the exception, even if we could have relied on the return
           value being None.

        """
        if self.generator:
            try:
                reply = await self.generator.asend(message["message"])
                if reply:
                    if isinstance(reply, dict):
                        return (
                            self.partner,
                            {
                                "action": "receive_reply",
                                "cid": self.cid,
                                "message": reply,
                                "sender": self.name,
                            },
                        )
                    else:
                        reply.cid = (
                            self.cid
                        )  # we need to inject a reference to the actual conversation
                        return reply
                else:
                    return (
                        self.partner,
                        {"action": "end_conversation", "cid": self.cid},
                    )
            except StopAsyncIteration:
                return (self.partner, {"action": "end_conversation", "cid": self.cid})
        else:
            pass

    async def close(self):
        """
        This method is invoked when, for some reason, the actor sees a need to
        close the conversation (such as a timeout, end request or some sort of
        error). This invokes an optionally supplied callback that can perform
        cleanup operations inside of the conversation and actor closures
        respectively.
        """
        if self.on_close:
            await self.on_close()
        else:
            pass

    def __call__(self, *args, **kwargs):
        return self.process_message(*args, **kwargs)


class Call(Conversation):
    """Presents a thin abstraction for nice calls"""

    def __init__(self, partner, greeting, on_close=None):
        super().__init__(partner, on_close=on_close)
        self.greeting = greeting

    async def attach(self, generator, name):
        await super().attach(generator, name)
        return {**self.greeting, "sender": name, "cid": self.cid}


class Respond(Conversation):
    """Presents a thin abstraction for nice responses"""

    def __init__(self, message, on_close=None):
        super().__init__(message["sender"], cid=message["cid"], on_close=on_close)
