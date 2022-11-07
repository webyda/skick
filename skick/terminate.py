"""
This module contains a simple mechanism for shutting down an asyncio
loop without cutting off any of the tasks' shutdown procedures. The need for
this arises because we can not simply gather all tasks on start up, since new
tasks are continuously being added from within other tasks.

The mechanism works as follows: When the loop is started, a Future is created.
the loop runs until the future is done.

The application stops by starting all of its cleanup procedures from within
some task, and then gathers all "stopping" tasks. When all tasks are done, the
future is set to done and the loop stops. This way, the task that initiates
the shutdown cascade does not have to be the last task to finish and we do not
risk cutting off some important data retention step or someting similar.
"""
import asyncio

switch = [] # Abusing mutability. Please don't tell anyone.

def set_loop(loop):
    switch.append(loop.create_future())
    return switch[0]

# We need to keep track of which "stopper" tasks are running. These tasks
# oversee the cleanup of some part of the system, such as an actor. It is
# important that terminate() knows which these tasks are so that it can gather
# only these particularly important tasks.
stopper_tasks = set()

def add_stopper(awt, loop):
    """
    When an actor or some other component has a "stopper" method, we wish to
    keep track of it in a central location. This function allows us to do that,
    by wrapping the awaitable in a coroutine that creates another task that
    creates an inner task for the awaitable, adds it to a set, and then removes
    it on completion. This way, we have a centralized list of all "stopper"
    tasks we need to await before we allow the event loop to terminate, but
    we also have someting of a headache because of the deeply nested set of
    tasks. This is the price we pay for a clean shutdown with the present
    architecture.
    """
    async def awt_wrapper():
        stopper = loop.create_task(awt)
        stopper_tasks.add(stopper)
        try:
            await stopper
        except: # This is the end of the world, so we can afford to be sloppy
            pass
        stopper_tasks.remove(stopper)
    stopper = loop.create_task(awt_wrapper())
    return stopper

async def terminate():
    await asyncio.gather(*(stopper_tasks-{asyncio.current_task()}))
    switch[0].set_result(True)
    