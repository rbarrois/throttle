# -*- coding: utf-8 -*-
# This code is distributed under the two-clause BSD license.
# Copyright (c) 2013 Raphaël Barrois


"""Base storage engines.

This module holds a few storage backends to be used for storing current bucket state.
"""


class BaseStorage(object):
    """Base class for a storage engine.

    Such objects provide a general abstraction over various storage backends
    (in-memory dict, remote cache, ...)
    """

    def get(self, key, default=None):
        """Retrieve the current value for a key.

        Args:
            key (str): the key whose value should be retrieved
            default (object): the value to use when no value exist for the key
        """
        raise NotImplementedError()

    def set(self, key, value):
        """Set a new value for a given key."""
        raise NotImplementedError()

    def mget(self, *keys, **kwargs):
        """Retrieve values for a set of keys.

        Args:
            keys (str list): the list of keys whose value should be retrieved

        Keyword arguements:
            default (object): the value to use for non-existent keys
            coherent (bool): whether all fetched values should be "coherent",
                i.e no other update was performed on any of those values while
                fetching from the database.

        Yields:
            object: values for the keys, in the order they were passed
        """
        default = kwargs.get('default')
        coherent = kwargs.get('coherent', False)
        for key in keys:
            yield self.get(key, default=default)

    def mset(self, values):
        """Set the value of several keys at once.

        Args:
            values (dict): maps a key to its value.
        """
        for key, value in values.items():
            self.set(key, value)

    def incr(self, key, amount, default=0):
        """Increment the value of a key by a given amount.

        Also works for decrementing it.

        Args:
            key (str): the key whose value should be incremented
            amount (int): the amount by which the value should be incremented
            default (int): the default value to use if the key was never set

        Returns:
            int, the updated value
        """
        old_value = self.get(key, default)
        self.set(key, old_value + amount)
        return old_value + amount


class DictStorage(BaseStorage):
    """A simple storage, backed by a dict."""
    def __init__(self):
        self.data = {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value


