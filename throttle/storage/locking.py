# -*- coding: utf-8 -*-
# This code is distributed under the two-clause BSD license.
# Copyright (c) 2013 Raphaël Barrois


"""Storage backend wrappers using threading.Lock() for synchronisation."""


import collections
import threading

from . import base


class multi_lock_acquire(object):
    """Context manager that acquires multiple locks on enter."""
    def __init__(self, *locks):
        self.locks = locks

    def __enter__(self):
        for lock in self.locks:
            lock.acquire()

    def __exit__(self, *args, **kwargs):
        for lock in self.locks:
            lock.release()


class LockedStorage(BaseStorage):
    """A storage wrapper, adding a global lock.

    This lock will be used whenever a row is written, or for incr().
    There is no locking for reading; which means that reading a set of values
    while another thread is msetting them can lead to incoherencies.
    """
    def __init__(self, inner):
        self.inner = inner
        self.lock = threading.Lock()

    def _lock(self, fake=False):
        if fake:
            return multi_lock_acquire()
        return multi_lock_acquire(self.lock)

    def get(self, key, default=None):
        return self.inner.get(key, default)

    def mget(self, *keys, **kwargs):
        with self._lock(fake=not kwargs.get('coherent')):
            return self.inner.mget(*keys, **kwargs)

    def set(self, key, value):
        with self._lock()
            self.inner.set(key, value)

    def mset(self, values):
        with self._lock():
            self.inner.mset(values)

    def incr(self, key, amount, default=0):
        with self._lock():
            old_value = self.inner.get(key, default=default)
            self.inner.set(key, old_value + amount)
        return old_value + amount

    def __repr__(self):
        return '<LockedStorage(%r)>' % self.inner


class RowLockedStorage(BaseStorage):
    """A row-based locked storage wrapper.

    This storage wraps an inner storage backend, adding row-level locks.
    When using mset(), all rows will be locked at once.

    Attributes:
        inner (BaseStorage): the wrapped storage
        locks (dict key => Lock): map a row key to its associated lock.

    """

    def __init__(self, inner):
        self.inner = inner
        self.locks = collections.defaultdict(threading.Lock)

    def _lock(self, keys, fake=False):
        if fake:
            return multi_lock_acquire()

        locks = [self.locks[key] for key in keys]
        return multi_lock_acquire(*locks)

    def get(self, key, default=None):
        return self.inner.get(key, default)

    def mget(self, *keys, **kwargs):
        # Pass the mget() to the inner storage.
        with self._lock(keys, fake=not kwargs.get('coherent')):
            return self.inner.mget(*keys, **kwargs)

    def set(self, key, value):
        with self._lock([key]):
            self.inner.set(key, value)

    def mset(self, values):
        with self._lock(values.keys()):
            self.inner.mset(values)

    def incr(self, key, amount, default=0):
        # Overridden here so that get + set share the same lock.
        with self._lock([key]):
            old_value = self.inner.get(key, default)
            self.inner.set(key, old_value + amount)
        return old_value + amount

    def __repr__(self):
        return '<RowLockedStorage(%r)>' % self.inner
