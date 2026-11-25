"""The keyspace: values, expiry times and the expiry machinery.

Values are stored as plain Python objects:

    string -> bytes
    list   -> collections.deque[bytes]
    hash   -> dict[bytes, bytes]
    set    -> set[bytes]

A key with a time to live is removed in two ways, like in Redis:

* lazily, when someone touches it after its deadline (``get`` checks);
* actively, by ``expire_cycle``, which the server runs ten times a second so
  that keys nobody reads again still free their memory.
"""
from __future__ import annotations

import heapq
import time
from collections import deque
from collections.abc import Callable, Iterator

Value = bytes | deque | dict | set

TYPE_NAMES = {bytes: "string", deque: "list", dict: "hash", set: "set"}


class Store:
    def __init__(self, clock: Callable[[], float] = time.time):
        self.clock = clock
        self.data: dict[bytes, Value] = {}
        self.expires: dict[bytes, int] = {}          # key -> deadline, unix time in ms
        self._heap: list[tuple[int, bytes]] = []     # (deadline, key), may hold stale entries
        self.expired_count = 0

    def now_ms(self) -> int:
        return int(self.clock() * 1000)

    # -- reading ---------------------------------------------------------------
    def get(self, key: bytes) -> Value | None:
        deadline = self.expires.get(key)
        if deadline is not None and deadline <= self.now_ms():
            self._remove(key)
            self.expired_count += 1
            return None
        return self.data.get(key)

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    def keys(self) -> Iterator[bytes]:
        now = self.now_ms()
        for key in list(self.data):
            deadline = self.expires.get(key)
            if deadline is None or deadline > now:
                yield key

    def __len__(self) -> int:
        return sum(1 for _ in self.keys())

    # -- writing ---------------------------------------------------------------
    def set(self, key: bytes, value: Value, keep_ttl: bool = False) -> None:
        """Store a value. Overwriting a key clears its TTL unless ``keep_ttl``."""
        if not keep_ttl:
            self.expires.pop(key, None)
        self.data[key] = value

    def delete(self, key: bytes) -> bool:
        if self.get(key) is None:
            return False
        self._remove(key)
        return True

    def drop_if_empty(self, key: bytes) -> None:
        """Redis never keeps empty lists, hashes or sets: the key disappears."""
        value = self.data.get(key)
        if value is not None and not isinstance(value, bytes) and not value:
            self._remove(key)

    def flush(self) -> None:
        self.data.clear()
        self.expires.clear()
        self._heap.clear()

    def _remove(self, key: bytes) -> None:
        self.data.pop(key, None)
        self.expires.pop(key, None)

    # -- expiry ----------------------------------------------------------------
    def expire_at(self, key: bytes, deadline_ms: int) -> bool:
        if self.get(key) is None:
            return False
        if deadline_ms <= self.now_ms():
            self._remove(key)  # a deadline in the past deletes the key at once
            return True
        self.expires[key] = deadline_ms
        heapq.heappush(self._heap, (deadline_ms, key))
        if len(self._heap) > 2 * len(self.expires) + 64:
            self._compact_heap()
        return True

    def persist(self, key: bytes) -> bool:
        return self.get(key) is not None and self.expires.pop(key, None) is not None

    def ttl_ms(self, key: bytes) -> int:
        """Milliseconds left; -2 if the key does not exist, -1 if it never expires."""
        if self.get(key) is None:
            return -2
        deadline = self.expires.get(key)
        return -1 if deadline is None else deadline - self.now_ms()

    def expire_cycle(self, budget: int = 1000) -> int:
        """Delete keys whose deadline has passed, at most ``budget`` per call.

        The heap is ordered by deadline, so only keys that are really due are
        looked at. An entry is stale if the key was deleted or got a new TTL
        since it was pushed; those are simply dropped. The budget stops one
        cycle from blocking the server when a million keys expire together.
        """
        now = self.now_ms()
        removed = 0
        heap = self._heap
        while heap and heap[0][0] <= now and removed < budget:
            deadline, key = heapq.heappop(heap)
            if self.expires.get(key) == deadline:
                self._remove(key)
                self.expired_count += 1
                removed += 1
        return removed

    def _compact_heap(self) -> None:
        self._heap = [(d, k) for k, d in self.expires.items()]
        heapq.heapify(self._heap)


def type_name(value: Value) -> str:
    return TYPE_NAMES[type(value)]
