#!/usr/bin/env python
# coding: utf-8

"""Tests for urlcleaner"""

import asyncio
import logging
import os
import unittest
import urllib

from aiohttp import test_utils
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
            'https://twitter.com/assaf'
        ]
        self.clean(urls)

    def clean(self, urls, **kwargs):
        with test_utils.run_server(self.loop) as httpd:
            server_url = httpd.url()

            if self.urlcleaner:
                self.urlcleaner.close()

            local_urls = [urllib.parse.urljoin(url, server_url) for url in
                          urls]
            self.urlcleaner = URLCleaner(local_urls, loop=self.loop, **kwargs)
            self.addCleanup(self.urlcleaner.close)
            self.loop.run_until_complete(self.urlcleaner.clean())
            self.urlcleaner.close()


if __name__ == '__main__':
    os.environ.setdefault('PYTHONASYNCIODEBUG', '1')
    if os.environ.get('LOGLEVEL') == 'DEBUG':
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logging.basicConfig(level=loglevel)
    unittest.main()
