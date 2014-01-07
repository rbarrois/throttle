# -*- coding: utf-8 -*-
# This code is distributed under the two-clause BSD license.
# Copyright (c) 2013 Raphaël Barrois


import mock
import time
import unittest

from throttle import buckets
from throttle.storage import base as storage_base


class BucketTest(unittest.TestCase):

    def test_keys(self):
        b = buckets.Bucket('foo', 2, 10, None)
        self.assertEqual('foo:current-amount', b.key_amount)
        self.assertEqual('foo:last-leak', b.key_last_leak)


class BucketLeakTest(unittest.TestCase):

    def setUp(self):
        self.storage = mock.Mock(spec=storage_base.BaseStorage)

    def test_leak_first(self):
        b = buckets.Bucket('foo', 2, 10, self.storage)
        self.storage.mget.return_value = (None, None)

        now = time.time()

        with mock.patch('time.time') as fake_time:
            fake_time.return_value = now
            res = b.leak()

        self.assertEqual(0, res)
        self.storage.mset.assert_called_once_with({
            'foo:current-amount': 0,
            'foo:last-leak': now,
        })

    def test_leak_empty_old(self):
        then = time.time() - 1000
        b = buckets.Bucket('foo', 2, 10, self.storage)
        self.storage.mget.return_value = (0, then)

        now = time.time()

        with mock.patch('time.time') as fake_time:
            fake_time.return_value = now
            res = b.leak()

        self.assertEqual(0, res)
        # Nothing should have changed
        self.storage.mset.assert_has_calls([])

    def test_leak_partial(self):
        now = time.time()
        then = now - 6
        b = buckets.Bucket('foo', 2, 30, self.storage)
        self.storage.mget.return_value = (20, then)

        # Decrease from 20 in 6 seconds at 2/s

        with mock.patch('time.time') as fake_time:
            fake_time.return_value = now
            res = b.leak()

        self.assertEqual(8, res)
        self.storage.mset.assert_called_once_with({
            'foo:current-amount': 8,
            'foo:last-leak': now,
        })

    def test_leak_below_zero(self):
        now = time.time()
        then = now - 6
        b = buckets.Bucket('foo', 2, 30, self.storage)
        self.storage.mget.return_value = (5, then)

        # Decrease from 20 in 6 seconds at 2/s

        with mock.patch('time.time') as fake_time:
            fake_time.return_value = now
            res = b.leak()

        self.assertEqual(0, res)
        self.storage.mset.assert_called_once_with({
            'foo:current-amount': 0,
            'foo:last-leak': now,
        })


class BucketConsumeTest(unittest.TestCase):
    def setUp(self):
        self.storage = mock.Mock(spec=storage_base.BaseStorage)

    def test_consume_single_empty(self):
        b = buckets.Bucket('foo', 2, 30, self.storage)
        b.leak = mock.Mock()
        b.leak.return_value = 0

        self.assertTrue(b.consume())
        self.storage.incr.assert_called_once_with('foo:current-amount', 1)

    def test_consume_single_partial(self):
        b = buckets.Bucket('foo', 2, 30, self.storage)
        b.leak = mock.Mock()
        b.leak.return_value = 10

        self.assertTrue(b.consume())
        self.storage.incr.assert_called_once_with('foo:current-amount', 1)

    def test_consume_single_full(self):
        b = buckets.Bucket('foo', 2, 30, self.storage)
        b.leak = mock.Mock()
        b.leak.return_value = 30

        self.assertFalse(b.consume())
        self.storage.incr.assert_has_calls([])

    def test_consume_multi_empty(self):
        b = buckets.Bucket('foo', 2, 30, self.storage)
        b.leak = mock.Mock()
        b.leak.return_value = 0

        self.assertTrue(b.consume(10))
        self.storage.incr.assert_called_once_with('foo:current-amount', 10)

    def test_consume_multi_partial(self):
        b = buckets.Bucket('foo', 2, 30, self.storage)
        b.leak = mock.Mock()
        b.leak.return_value = 10

        self.assertTrue(b.consume(10))
        self.storage.incr.assert_called_once_with('foo:current-amount', 10)

    def test_consume_multi_full(self):
        b = buckets.Bucket('foo', 2, 30, self.storage)
        b.leak = mock.Mock()
        b.leak.return_value = 25

        self.assertFalse(b.consume(10))
        self.storage.incr.assert_has_calls([])


class BucketEndToEndTest(unittest.TestCase):
    def setUp(self):
        self.storage = mock.Mock(spec=storage_base.BaseStorage)

    def test_first_time(self):
        b = buckets.Bucket('foo', 2, 10, self.storage)
        self.storage.mget.return_value = (None, None)

        now = time.time()

        with mock.patch('time.time') as fake_time:
            fake_time.return_value = now
            res = b.consume()

        self.assertTrue(res)
        self.storage.incr.assert_called_once_with('foo:current-amount', 1)
        self.storage.mset.assert_called_once_with({
            'foo:current-amount': 0,
            'foo:last-leak': now,
        })

    def test_empty_old(self):
        now = time.time()
        then = now - 1000

        b = buckets.Bucket('foo', 2, 10, self.storage)
        self.storage.mget.return_value = (0, then)

        with mock.patch('time.time') as fake_time:
            fake_time.return_value = now
            res = b.consume()

        self.assertTrue(res)
        self.storage.incr.assert_called_once_with('foo:current-amount', 1)
        self.storage.mset.assert_called_once_with({
            'foo:current-amount': 0,
            'foo:last-leak': now,
        })
