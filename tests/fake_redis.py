"""A tiny in-memory stand-in for the Redis list commands the queue uses.

Index 0 is the head (left) of a list, like real Redis. Values are stored as bytes
because that's what the real client returns. brpop really waits (on a Condition)
so worker tests can check blocking behaviour and stopping from another thread.
"""

import threading


class FakeRedis:
    def __init__(self):
        self.lists = {}
        self.list_changed = threading.Condition()

    def lpush(self, key, *values):
        with self.list_changed:
            items = self.lists.setdefault(key, [])
            for value in values:
                if isinstance(value, str):
                    value = value.encode("utf-8")
                items.insert(0, value)
            self.list_changed.notify_all()
            return len(items)

    def llen(self, key):
        with self.list_changed:
            return len(self.lists.get(key, []))

    def lrange(self, key, start, end):
        with self.list_changed:
            items = self.lists.get(key, [])
            # Redis treats end as inclusive and -1 as "last element".
            if end == -1:
                return list(items[start:])
            return list(items[start : end + 1])

    def brpop(self, keys, timeout=0):
        """Pop from the tail of the first non-empty list, waiting up to timeout seconds."""
        with self.list_changed:
            while True:
                for key in keys:
                    items = self.lists.get(key)
                    if items:
                        return (key.encode("utf-8"), items.pop())
                # wait() returns False when it times out without being notified.
                if not self.list_changed.wait(timeout=timeout if timeout else None):
                    return None
