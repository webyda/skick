"""
This module contains a simple mechanism for shutting down an asyncio
loop without cutting off any of the tasks' shutdown procedures. The need for
this arises because we can not simply gather all tasks on start up, since new
tasks are continuously being added from within other tasks.

The mechanism works as follows: When the loop is started, a Future is created.
The loop runs until the future is done. The application stops by starting all
of its cleanup procedures from within some task, and then gathers all
"stopping" tasks. When all tasks are done, the future is set to done and the
loop stops. This way, the task that initiates the shutdown cascade does not
have to be the last task to finish and we do not risk cutting off some
important data retention step or someting similar.
"""
import asyncio
import traceback

switch = []  # Abusing mutability. Please don't tell anyone.


def set_loop(loop):
    """
    We need to know the identity of the event loop to be able to instantiate
    the "switch" future. Therefore, we refrain from doing so until the skick
    instance creates the loop.
    """
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
    keep track of it in a central location. This function allows us to do that.
    This enables us to know which tasks to gather when we want to terminate the
    entire program, and which ones we can simply cancel.

    Note to self: Big Boys (tm) don't necessarily check for exceptions where they occur.
    This is because we might be relying on catching them at some other point in the code.

    Extra note to self: This function needs to be rewritten, but it sort of works at the moment.
    """
    fut = asyncio.Future(loop=loop)

    async def awt_wrapper():
        stopper = loop.create_task(awt)
        waiter = stopper
        stopper_tasks.add(stopper)
        try:
            await stopper
        finally:
            stopper_tasks.remove(stopper)
            if not fut.done():
                fut.set_result(True)

    stopper = loop.create_task(awt_wrapper())
    return fut, stopper


async def terminate():
    """
    Collates all tasks that have been marked as "stopping" tasks and awaits
    them (excluding the current task). When all tasks are done, flip the switch
    in the singleton module, signalling that the loop can be stopped safely.
    """
    await asyncio.gather(
        *(stopper_tasks - {asyncio.current_task()}), return_exceptions=True
    )
    switch[0].set_result(True)
