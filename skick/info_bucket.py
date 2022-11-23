"""
Contains a class for managing time stamped information about shards' facilities.
For example, which services they provide and whether they can spawn a
particular type of actor.
"""

import asyncio


class InfoBucket:
    """
    InfoBuckets are containers for synchronising data packets
    from inter-shard communications. They contain a "key", timestamp and
    info field. They do not represent a *state* per se, but a piece of
    information about state. Several contradictory pieces of information about
    the same key may float around in the cluster at the same time.

    The InfoBucket helps us keep track of these informations by discarding all
    but the most up to date information it receivces. Since erroneous
    information is not produced independently at non-affected nodes, we know
    that newer more accurate information will eventually replace older
    inaccurate information that has been floating around in the network.
    """

    def __init__(self):
        self.informations = {}  # key -> [timestamp, info]

    def add_information(self, info):
        """
        Adds an "information", that is, a dictionary of the form
        {"key": dict, "timestamp": float, "info": dict}. Since dictionaries
        are not hashable, we have to cast the dictionary into a frozenset of
        key value pairs before we can use it as a key. This is sad.
        """
        key = frozenset(info["key"].items())

        if key not in self.informations:
            self.informations[key] = [info["timestamp"], info["info"]]
        elif info["timestamp"] > self.informations[key][0]:
            self.informations[key] = [info["timestamp"], info["info"]]
        else:
            pass

    def consume_batch(self, batch):
        """Takes a batch of informations and adds them to the bucket"""
        for info in batch:
            self.add_information(info)

    def purge(self, age, key_filter=None, info_filter=None):
        """
        Purges information older than some particular timestamp, provided that
        it complies with the key_filter and info_filter functions (when
        present).
        """
        tmp = self.informations.items()

        if key_filter:
            tmp = filter(lambda x: key_filter(x[0]), tmp)
        if info_filter:
            tmp = filter(lambda x: info_filter(x[1][1]), tmp)
        tmp = filter(lambda x: x[1][0] > age, tmp)

        self.informations = dict(tmp)

    def extract(self, filter_func=None, map_func=None, to_dict=True, listify=False):
        """
        Applies some transformations to the information in the bucket and
        collates and returns the result. The options are:
        1. filter_func: a function that is true for the information we want to process
        2. map_func: a function to transform a filtered information into something else
        3. to_dict: whether to convert the result to a dictionary with the dict() builtin
        4. listify: whether there are several rows with the same key, where
                    the values are supposed to be list entries under the same
                    key. If it is an iterable, these are presumed to be a partial
                    list of keys that must be in the resulting dictionary (in
                    other words, this is a way to inject keys which may have
                    empty lists but still need to be present).
        """

        ret = [(dict(key), val) for key, val in self.informations.items()]
        ret = filter(filter_func, ret)
        ret = list(map(map_func, ret))
        if to_dict:
            if not listify:
                return dict(ret) or {}
            else:
                if isinstance(listify, bool):
                    tmp = {}
                else:
                    tmp = {key: [] for key in listify}

                for key, val in ret:
                    if key not in tmp:
                        tmp[key] = [val]
                    else:
                        tmp[key].append(val)
                return tmp

        else:
            return ret or []

    def export(self):
        """
        Converts the frozenset keys back to dictionaries to build nice JSON
        friendly objects.
        """

        return [
            {"key": dict(k), "timestamp": v[0], "info": v[1]}
            for k, v in self.informations.items()
        ]
