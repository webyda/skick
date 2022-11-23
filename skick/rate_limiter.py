"""
Contains a simple rate limiter class which can be awaited repeatedly in order
to prevent some block of code from being run too frequently.
"""

import asyncio
from abc import ABC, abstractmethod
from time import monotonic


class RateExceeded(Exception):
    """
    Raised by a rate limiter to suggest that the user has exceeded the
    maximum allowed rate and its connection should be terminated.
    """


class RateLimiter(ABC):
    def __init__(self):
        self.lock = asyncio.Lock()

    async def __call__(self) -> float:
        """
        When this method is awaited, some magic possibly stateful operation will
        be performed that will produce a float with a suggested delay. The
        limiter may also throw a RateExceeded exception indicating that it
        believes the user to be malicious.

        We will hide the actual implementation behind an asynio.Lock to avoid
        the risk of race conditions in the rate limiter.
        """
        async with self.lock:
            return await self._limiter()

    async def waiter(self):
        """Automatically wait"""
        verdict = await self()
        await asyncio.sleep(verdict)
        return bool(verdict)

    @abstractmethod
    async def _limiter(self) -> float:
        """
        Some method for deciding whether to suggest a pause or not.
        """


class TokenBucket(RateLimiter):
    """
    Lets the user perform actions while depleting a "bucket" full of tokens.
    Once the bucket has less than one token, the user will be sanctioned
    with a "punishment". If they keep taking tokens, their debt will
    reach a maximum debt and the bucket will throw an exception.
    """

    def __init__(self, capacity=10.0, fill_rate=2.0, punishment=0.1, max_debt=-10.0):
        super().__init__()
        self.max_debt = max_debt
        self.capacity = capacity
        self.fill_rate = fill_rate
        self.tokens = capacity
        self.punishment = punishment
        self.last_time = monotonic()

    async def _limiter(self):
        """
        Adds tokens to the bucket, then looks if there are any in the bucket.
        If there are, it takes one and returns immediately. If there are none,
        it returns a suggested punishment in seconds. If the maximum
        debt has been reached, it raises a RateExceeded exception.
        """
        now = monotonic()
        self.tokens = min(
            self.capacity, self.tokens + (now - self.last_time) * self.fill_rate
        )

        if self.tokens < 1.0:
            if self.tokens < self.max_debt:
                raise RateExceeded()
            else:
                self.tokens -= 1.0
                self.last_time = now
                return self.punishment
        else:
            self.tokens -= 1.0
            self.last_time = now
            return 0.0
