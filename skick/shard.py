"""
Contains a function that generates a shard actor. The shard actor is mostly
concerned with keeping track of the state of the shard's internal resources and
synchronizing the cluster_data object's state with the other shards in the
cluster. It is also tasked with spawning and supervising actors which are not
subsessions.
"""
import asyncio

from random import choice, random, sample
from math import log
from traceback import print_exc
from typing import Callable, Type, Awaitable
from itertools import chain

from schema import Const, Optional, Or

from .message_system_interface import MessageSystemInterface
from .actor import Actor
from .conversation import Respond, Call
from .addressing import get_address

from . import terminate
from .cluster import cluster_data


def shard(
    actor_base: Type[Actor],
    message_factory: Callable[[], Awaitable[MessageSystemInterface]],
    shard_id: str = None,
    loop=None,
) -> Actor:
    """
    Builds a special actor called a "shard" actor. The shard actor manages the
    local process and synchronizes it with the other shards in the cluster.
    """
    msg_sys = message_factory()
    shard_actor = actor_base(
        shard_id if shard_id else get_address(is_shard=True), msg_sys, loop=loop
    )

    cluster_data.shard = shard_actor.name
    actors = {}  # This keeps track of all managed actor instances

    message_watcher = None

    @shard_actor.on_start
    async def onstart():
        nonlocal message_watcher
        # First, we need to announce the presence of the shard to the messaging system
        await msg_sys.register_shard(shard_actor.name)

        # Second, we need to attach a watcher to the _message_task of the shard
        # so that it can handle its own exceptions. This is necessary because
        # the shard is the top actor in the supervision tree.
        async def watcher():
            try:
                await shard_actor._message_task
            except Exception as e:
                print(print_exc())
                print(f"Shard {shard_actor.name} died with exception {e}")
                await shard_actor.stop()

        message_watcher = shard_actor.loop.create_task(watcher())

    @shard_actor.action("ping")
    async def ping(msg):
        """Responds to other shards' pings so that they know it is still alive"""
        yield Respond(msg)
        yield {"message": "pong"}

    @shard_actor.daemon("pinger", restart=True)
    async def pinger():
        """
        Pings a random shard with some set interval. If the shard does not
        reply before a set timeout, it is assumed to be dead. If it later
        turns out to be alive, it will reintroduce itself.
        """

        async def conv(msg):
            """
            A conversation that helps us determine if the shard is alive.
            The calling shard waits for a response, and as soon as it arrives,
            it sets a supplied future. The furure is awaited by some task
            and is on a timeout.
            """
            fut = msg["future"]  # This is a hack
            response = yield Call(msg["recipient"], {"action": "ping"})
            fut.set_result(True)

        async def waiter():
            """
            The idea is to have all shards query some other random shard
            they believe to be alive. Theoretically, this should mean that all
            shards are eventually pinged. The process is handled by a task that
            awaits a future that is set by the conversation above. If the
            asyncio.wait_for times out, it means that the shard is presumed dead.
            and it will be marked as dead in the cluster_data object.
            """
            try:
                target = choice(list(cluster_data.remote_shards))
                fut = shard_actor.loop.create_future()
                await shard_actor.converse({"recipient": target, "future": fut}, conv)
                await asyncio.wait_for(fut, timeout=5)

            except asyncio.TimeoutError:
                cluster_data.kill_shard(target)
                cluster_data.decode_bucket()
            except IndexError:
                # This happens if some other task has removed the shard between
                # our spawning the task and the task actually running.
                # It is benign, so we simply ignore it.
                pass

        while True:
            await asyncio.sleep(5)
            if cluster_data.remote_shards:
                shard_actor.loop.create_task(waiter())

    @shard_actor.daemon("whisperer", restart=True)
    async def whisperer(whisper_interval=1):
        """
        Regularly whispers known information to adjacent shards
        the algorithm picks random shards from the list of remote shards
        and sends one copy of the data to a shard with a lower address
        and one copy to a shard with a higher address. This is repeated with
        a set time interval. This ensures topological mixing and allows us to
        concentrate the data at the edges of the address space.
        """
        while True:
            await asyncio.sleep(whisper_interval)

            lower = [
                shard
                for shard in cluster_data.remote_shards
                if shard < shard_actor.name
            ]
            upper = [
                shard
                for shard in cluster_data.remote_shards
                if shard > shard_actor.name
            ]

            recip = chain(
                sample(
                    lower,
                    k=min(len(lower), int(log(len(cluster_data.remote_shards) + 1))),
                ),
                sample(
                    upper,
                    k=min(len(upper), int(log(len(cluster_data.remote_shards) + 1))),
                ),
            )
            cluster_data.update_bucket()
            info_batch = cluster_data.transmission()
            for recipient in recip:
                await shard_actor.send(
                    recipient,
                    {
                        "action": "receive_info",
                        "batch": info_batch,
                        "purge": False,
                        "broadcaster": shard_actor.name,
                    },
                )

    @shard_actor.daemon("broadcaster")
    async def broadcaster():
        """
        Regularly broadcasts known information to all shards using the
        message system's broadcast functionality. This should be done with some
        probability which is inversely proportional to the rank the shard has in
        the address space of its neighbors, but for the time being this feature
        has not been implemented since no suitable distribution has been reserached.
        This should only become a problem for use cases where there are very many shards,
        possibly hundreds or thousands.
        """
        while True:
            await asyncio.sleep(1)
            # use some smart distribution in the future. For now, just use a
            # uniform distribution

            if random() < 1 / (len(cluster_data.remote_shards) + 1):
                cluster_data.update_bucket()
                info_batch = cluster_data.transmission()
                await msg_sys.broadcast(
                    {
                        "action": "receive_info",
                        "batch": info_batch,
                        "purge": True,
                        "broadcaster": shard_actor.name,
                    }
                )
            else:
                pass

    @shard_actor.action(
        "receive_info",
        schema={
            "action": Const("receive_info"),
            "batch": [{"key": dict, "timestamp": float, "info": Or(bool, dict)}],
            "purge": bool,
            "broadcaster": str,
        },
    )
    async def receive_info(message):
        """
        Receives information from other shards and updates the information
        records accordingly. If the purge flag is set, the information records
        are purged of all data on deceased nodes that are older than some
        number of seconds (presumably no nodes should believe these old shards are still
        alive at this point). Purged obituaries will be reintroduced by
        the concerned shards if they receive a notice that they possess a service
        that they know not to be present.
        """

        if not message["broadcaster"] == shard_actor.name:
            cluster_data.consume_batch(message["batch"], message["purge"])
            cluster_data.decode_bucket()
        else:
            if message["purge"]:
                cluster_data.purge()
            else:
                pass

    async def spawn(actor_type, message={}, name=None, monitors=None, run=True):
        """
        This function spawns a new actor, returns its name and adds it to the
        appropriate supervision trees, if any, and starts it.
        """
        if actor_type in cluster_data.local_factories:
            actor_type, actor_factory = cluster_data.local_factories[actor_type]
            name = name if name else get_address()

            actor = actor_type(
                name,
                message_system=message_factory(),
                loop=loop,
                shard=shard_actor.name,
            )

            actor._injected_spawn = injectable_spawn
            actor_factory(actor, message)

            actors[name] = actor
            await actor._monitor({"address": shard_actor.name})

            if monitors:
                if isinstance(monitors, str):
                    await actor._monitor({"address": monitors})
                else:
                    for m in monitors:
                        await actor._monitor({"address": m})
            if run:
                await actor.run()
                return name
            else:
                return name

        else:
            print(
                f"Actor type {actor_type} not found in {cluster_data.local_factories}"
            )
            return None

    shard_actor.spawn = spawn

    async def injectable_spawn(
        actor_type, message={}, name=None, monitors=None, remote="mixed", run=True
    ):
        """
        To be injected into actors to allow them to spawn on the shard without
        having to go through the messaging system. This is necessary for a few
        reasons, the most important being that the actor needs to be able to
        figure out the address of the new actor without having to wait for
        another message to arrive. Since the shard can know in advance what the
        name of the new actor will be, this is a good compromise.
        """
        if remote == "local":
            return await spawn(actor_type, message, name, monitors, run)
        elif remote == "mixed":
            if actor_type in cluster_data.local_factories:
                return await spawn(actor_type, message, name, monitors, run)
            elif actor_type in cluster_data.remote_factories:
                shard = choice(cluster_data.remote_factories[actor_type])
                name = get_address(shard=shard)
                await shard_actor.send(
                    shard,
                    {
                        "action": "spawn",
                        "type": actor_type,
                        "name": name,
                        "message": message,
                        "add_monitor": monitors,
                        "remote": "local",
                    },
                )
                return name
            else:
                print(
                    f"No remote factory named {actor_type} in dictionary {cluster_data.remote_factories}"
                )
        elif remote == "remote":
            shard = choice(cluster_data.remote_factories[actor_type])
            name = get_address(shard=shard)
            await shard_actor.send(
                shard,
                {
                    "action": "spawn",
                    "type": actor_type,
                    "name": name,
                    "message": message,
                    "add_monitor": monitors,
                    "remote": "local",
                },
            )
            return name
        else:
            print("remote parameter is ill formed")
            return None

    shard_actor._injected_spawn = injectable_spawn

    @shard_actor.action(
        "spawn",
        schema={
            "action": Const("spawn"),
            "type": str,
            Optional("name"): str,
            Optional("message"): dict,
            Optional("add_monitor"): Or([str], str),
            Optional("remote"): str,
        },
    )
    async def spawn_from_message(message):
        """
        Actors can be spawned after a request sent to the shard over the message
        system. This is primarily used for spawning actors on remote shards.
        """
        act_type = message["type"]
        name = message["name"] if "name" in message else get_address()
        msg = message["message"] if "message" in message else {}
        remote = message["remote"] if "remote" in message else "mixed"

        match remote:
            case "mixed":
                if act_type in cluster_data.local_factories:
                    await spawn(
                        act_type,
                        message=msg,
                        name=name,
                        monitors=message["add_monitor"],
                    )
                elif act_type in cluster_data.remote_factories:
                    await shard_actor.send(
                        choice(cluster_data.remote_factories[act_type]),
                        {**message, "remote": "local"},
                    )

                else:
                    print(f"Incorrect actor type {act_type} requested")

            case "local":
                if act_type in cluster_data.local_factories:
                    await spawn(
                        act_type,
                        message=msg,
                        name=name,
                        monitors=message["add_monitor"],
                    )
                else:
                    print(f"Incorrect actor type {act_type}")
            case "remote":
                if act_type in cluster_data.remote_factories:
                    await shard_actor.send(
                        choice(cluster_data.remote_factories[act_type]),
                        {**message, "remote": "local"},
                    )
                else:
                    print(f"Incorrect actor type {act_type}")

    def factory(name, actor_type=actor_base):
        """Attaches a factory to the cluster_data object"""

        def decorator(func):
            cluster_data.local_factories[name] = (actor_type, func)
            return func

        return decorator

    shard_actor.actor = factory  # Exposes an interface to the programmer

    @shard_actor.default_sentinel
    async def default_sentinel(msg):
        """
        Responsible for handling the death of spawned actors in a responsible
        and dignified manner by removing them from the list of actors and
        by announcing their death through an obituary in the info_bucket if
        they provided a service.
        """
        addr = msg.get("address", None)

        if addr:
            if addr in actors:
                del actors[addr]

            cluster_data.delete_service(addr)
        else:
            pass

    @shard_actor.on_stop
    async def onstop():
        """
        Creates and broadcasts the required obituaries and kills the actors
        """
        try:
            if shard_actor.ws_actor:
                # If a kind soul has injected a websocket actor. Kill it.
                await shard_actor.ws_actor.stop()

            cluster_data.update_bucket(kill=True)

            for actor in actors.values():
                await actor.stop()

            await msg_sys.unregister_shard(shard_actor.name)

            # Say a final goodbye to all the old shard pals
            info_batch = cluster_data.transmission()

            await msg_sys.broadcast(
                {
                    "action": "receive_info",
                    "batch": info_batch,
                    "purge": False,
                    "broadcaster": shard_actor.name,
                }
            )

            await terminate.terminate()

        except Exception as e:
            print(print_exc())

    return shard_actor


Shard = shard
