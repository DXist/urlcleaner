#!/usr/bin/env python

# coding: utf-8

"""Tests for urlcleaner"""

import asyncio
import logging
import os
import unittest

from urlcleaner import URLCleaner


class TestURLCleaner(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.addCleanup(self.loop.close)
        self.urlcleaner = None

    def test_ok(self):
        urls = [
            'https://twitter.com/anilkirbas',
            'https://www.twitter.com/rsk_living',
            'https://twitter.com/assaf',
        ]
        self.clean(urls)

    def test_bad_data(self):
        urls = [
            '@anilkirbas',
            'https://www.twitter.com/some_test_url',
            'https://noname.noname',
            'https://localhost:2',
            None,
        ]
        self.clean(urls)

    def clean(self, urls, **kwargs):
        self.urlcleaner = URLCleaner(urls, loop=self.loop, **kwargs)
        self.loop.run_until_complete(self.urlcleaner.clean())


if __name__ == '__main__':
    os.environ.setdefault('PYTHONASYNCIODEBUG', '1')
    if os.environ.get('LOGLEVEL', 'DEBUG') == 'DEBUG':
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logging.basicConfig(level=loglevel)
    unittest.main()
