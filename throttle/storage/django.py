# -*- coding: utf-8 -*-
# This code is distributed under the two-clause BSD license.
# Copyright (c) 2013 Raphaël Barrois


from django.core import cache as django_cache

from . import base


class DjangoCacheStorage(base.BaseStorage):
    """A django cache-based storage.

    Attributes:
        cache_name (str): the name of the cache backend to use
        cache: the actual django cache backend
    """

    def __init__(self, cache_name, **kwargs):
        self.cache_name = cache_name
        self._cache = None
        super(DjangoCacheStorage, self).__init__(**kwargs)

    @property
    def cache(self):
        """Memoize access to the cache backend."""
        if self._cache is None:
            self._cache = django_cache.get_cache(self.cache_name)
        return self._cache

    def get(self, key, default=None):
        return self.cache.get(key, default)

    def set(self, key, value):
        self.cache.set(key, value)

    def mget(self, *keys, **kwargs):
        values = self.cache.get_many(keys)
        for key in keys:
            yield values[key]

    def mset(self, values):
        self.cache.set_many(values)

    def incr(self, key, amount, default=0):
        # Ensure the key exists
        self.cache.add(key, default)

        return self.cache.incr(key, amount)
