"""
This module contains a singleton for managing cluster information. It was added
to simplify operations such as requesting a service and to make the shard actor
a little cleaner. It is technically not a satisfying solution to have non-actor
components in a library like this, but practicalities are preferred over
purity in principle.
"""
import asyncio
from .info_bucket import InfoBucket
from time import monotonic


class ClusterDataManager:
    """
    This class maintains a local database of which resources are available in
    the cluster and where they can be found. It does this by maintaining an
    internal InfoBucket where it stores timestamped information about the
    availability of particular resources like a service. The bucket is then
    used to construct dictionaries of information for easy and convenient lookup.
    """

    def __init__(self, shard=None):
        self.infos = InfoBucket()

        self.shard = None
        self.local_services = {}  # This keeps track of all local services
        self.local_factories = {}  # This keeps track of all local factories
        self.remote_services = {}  # This keeps track of all remote services
        self.remote_shards = {}
        self.remote_factories = {}  # Keeps track of which shards can spawn which actors

    def purge(self, ttl=20):
        """Purges all information older than ttl seconds"""
        self.infos.purge(monotonic() - ttl)

    def consume_batch(self, batch, purge=False, ttl=20):
        """Takes a list of information dicts and adds them to the bucket."""
        self.infos.consume_batch(batch)
        if purge:
            self.purge(ttl=ttl)
        self.update_bucket()

    def add_service(self, service, address):
        """Adds a local service"""
        if service in self.local_services:
            self.local_services[service].add(address)
        else:
            self.local_services[service] = {address}

    def delete_service(self, address):
        """Removes an actor from the local service registry"""
        for service, addresses in self.local_services.items():
            if address in addresses:
                self.local_services[service].remove(address)
                self.infos.add_information(
                    {
                        "key": {
                            "shard": self.shard,
                            "service": service,
                            "address": address,
                        },
                        "timestamp": monotonic(),
                        "info": False,
                    }
                )
            else:
                pass

    def transmission(self):
        """Produces a fresh broadcastable batch of information."""
        return self.infos.export()

    def update_bucket(self, kill=False):
        """
        Updates the contents of the InfoBucket with local knowledge.
        """
        self.make_consistent()
        timestamp = monotonic()
        for service, providers in self.local_services.items():
            for provider in providers:
                self.infos.add_information(
                    {
                        "key": {
                            "shard": self.shard,
                            "service": service,
                            "address": provider,
                        },
                        "timestamp": timestamp,
                        "info": not kill,
                    }
                )

        self.infos.add_information(
            {"key": {"shard": self.shard}, "timestamp": timestamp, "info": not kill}
        )

        for factory in self.local_factories:
            self.infos.add_information(
                {
                    "key": {"shard": self.shard, "factory": factory},
                    "timestamp": timestamp,
                    "info": not kill,
                }
            )

    def decode_bucket(self):
        """
        Uses the information in the bucket to update the local database.
        """

        def filter_remote_services(info):
            """True for keys representing remote services"""
            key, (_, alive) = info
            return "service" in key and alive and not key["shard"] == self.shard

        def map_remote_services(info):
            key, _ = info
            return (key["service"], key["address"])

        self.remote_services = self.infos.extract(
            filter_func=filter_remote_services,
            map_func=map_remote_services,
            to_dict=True,
            listify=True,
        )

        def filter_shards(info):
            """Returns true for keys representing shards other than the current one"""
            key, (_, alive) = info
            return (
                "shard" in key
                and len(key) == 1
                and not key["shard"] == self.shard
                and alive
            )

        def map_shard(info):
            """Produces a dictionary of active and inactive shards"""
            key, (_, alive) = info
            return key["shard"]

        shards = set(
            self.infos.extract(
                filter_func=filter_shards, map_func=map_shard, to_dict=False
            )
        )

        def filter_shard_services(info):
            """True for active services on nonlocal shards"""
            key, (_, alive) = info
            return "service" in key and key["shard"] in shards and alive

        def map_shard_services(info):
            """Maps the information to a dictionary of shards and services"""
            key, _ = info
            return (key["shard"], key["service"])

        self.remote_shards = self.infos.extract(
            filter_func=filter_shard_services,
            map_func=map_shard_services,
            to_dict=True,
            listify=(shards or True),
        )

        def filter_factories(info):
            """True for active factories on living shards"""
            key, _ = info
            return (
                "factory" in key
                and not key["shard"] == self.shard
                and key["shard"] in shards
            )

        def map_factories(info):
            """Maps the information to a dictionary of factories"""
            key, _ = info
            return key["factory"], key["shard"]

        self.remote_factories = self.infos.extract(
            filter_func=filter_factories,
            map_func=map_factories,
            to_dict=True,
            listify=True,
        )

        # Finally, attempt to detect wether there is any false information about
        # our current shard in the bucket. If so, replace it with an up to date
        # correction. Specifically, try to detect if there are any services
        # marked as alive which are actually dead.
        timestamp = monotonic()

        def filter_erroneous_locals(info):
            """
            Filter out actors on our shard which are maked as alive, but
            which are, in fact, not present.
            """
            key, (_, alive) = info
            return (
                key["shard"] == self.shard
                and "service" in key
                and key["address"] not in self.local_services[key["service"]]
                and alive
            )

        def map_corrections(info):
            """Constructs a set of dictionaries with corrected information"""
            key, _ = info
            return {"key": key, "timestamp": timestamp, "info": False}

        corrections = self.infos.extract(
            filter_func=filter_erroneous_locals, map_func=map_corrections, to_dict=False
        )
        self.infos.consume_batch(corrections)

    def kill_shard(self, shard):
        """
        Kills a remote shard by marking all its resources as dead.
        """
        timestamp = monotonic()

        def filter_shard(info):
            """True for all data about the given shard being "alive"""
            key, (_, alive) = info
            return key.get("shard", None) == shard and alive

        def map_shard(info):
            key, _ = info
            return {"key": key, "timestamp": timestamp, "info": False}

        corrections = self.infos.extract(
            filter_func=filter_shard, map_func=map_shard, to_dict=False
        )
        self.infos.consume_batch(corrections)

    def make_consistent(self):
        """
        Looks at the present state of the bucket and attempts to make it
        consistent. Namely, it will attempt to detect living services and
        factories on dead shards.
        """
        timestamp = monotonic()

        # First compile a set of dead shards
        def filter_dead_shards(info):
            """True for all data about dead shards"""
            key, (_, alive) = info
            return len(key) == 1 and not alive

        def map_dead_shards(info):
            """Returns the name of the dead shard"""
            key, _ = info
            return key["shard"]

        dead_shards = set(
            self.infos.extract(
                filter_func=filter_dead_shards, map_func=map_dead_shards, to_dict=False
            )
        )

        # Next make a list of corrections marking "living" entities as dead
        # if they live on a dead shard
        def filter_inconsistent(info):
            """Finds living services and factories on dead shards"""
            key, (_, alive) = info
            return key["shard"] in dead_shards and alive

        def map_inconsistent(info):
            """Marks the undead as dead."""
            key, (_, alive) = info
            return {"key": key, "timestamp": timestamp, "info": False}

        corrections = self.infos.extract(
            filter_func=filter_inconsistent, map_func=map_inconsistent, to_dict=False
        )

        # Consume the corrections
        self.infos.consume_batch(corrections)


cluster_data = ClusterDataManager()  # We usually import this directly
