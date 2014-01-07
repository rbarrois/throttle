# -*- coding: utf-8 -*-
# This code is distributed under the two-clause BSD license.
# Copyright (c) 2013 Raphaël Barrois


import time


class Bucket(object):
    """A bucket.

    A bucket "leaks" queries at a rate of ``rate`` per second,
    and has a maximum capacity of ``capacity``.

    When a new request arrives (call to ``consume()``), the bucket checks that
    it can add it without exceeding capacity; otherwise, it is rejected.

    Attributes:
        key (str): the base namespace used by this bucket
        rate (float): the rate at which consumed units are regenerated,
            in units per second
        capacity (int): the maximum amount of units in the bucket
        storage (BaseStorage): a storage engine for this bucket
    """

    def __init__(self, key, rate, capacity, storage, **kwargs):
        super(Bucket, self).__init__(**kwargs)
        self.key = key
        self.rate = rate
        self.capacity = capacity

        self.storage = storage

    @property
    def key_amount(self):
        """Name of the key used to store the current amount."""
        return '%s:%s' % (self.key, 'current-amount')

    @property
    def key_last_leak(self):
        """Name of the key used to store the time of the last leak."""
        return '%s:%s' % (self.key, 'last-leak')

    def leak(self):
        """Leak the adequate amount of data from the bucket.

        This should be called before any consumption takes place.

        Returns:
            int: the new capacity of the bucket
        """
        capacity, last_leak = self.storage.mget(self.key_amount, self.key_last_leak,
                coherent=True)

        now = time.time()

        if last_leak:
            elapsed = now - last_leak
            decrement = elapsed * self.rate

            new_capacity = max(int(capacity - decrement), 0)
        else:
            new_capacity = 0

        self.storage.mset({
            self.key_amount: new_capacity,
            self.key_last_leak: now,
        })

        return new_capacity

    def _incr(self, amount):
        """Handle an atomic increment to the bucket."""
        self.storage.incr(self.key_amount, amount)

    def consume(self, amount=1):
        """Consume one or more units from the bucket."""
        # First, cleanup old stock
        current = self.leak()

        if current + amount > self.capacity:
            return False

        self._incr(amount)
        return True
