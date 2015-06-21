#!/usr/bin/env python
# coding: utf-8

"""Tests for urlcleaner"""

import logging
import os
import unittest

import httpretty
from urlcleaner import check_urls



class TestURLCleaner(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.addCleanup(self.loop.close)
        self.urlcleaner = None

    @httpretty.activate
    def test_ok(self):
        urls = [
            'https://twitter.com/anilkirbas',
            'https://www.twitter.com/rsk_living',
            'https://twitter.com/assaf'
        ]
        for url in urls[:-1]:
            httpretty.register_uri(httpretty.HEAD, url, status=200)

        httpretty.register_uri(urls[-1], url, status=404)

        def urlreader():
            yield from urls

    def clean(self, **kwargs):
        if self.urlcleaner:
            self.urlcleaner.close()
        self.urlcleaner = URLCleaner(urlreader=urlreader(), loop=self.loop)
        self.addCleanup(urlcleaner.clean)
        self.loop.run_until_complete(self.urlcleaner.clean)


if __name__ == '__main__':
    os.environ['PYTHONASYNCIODEBUG'] = '1'
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
