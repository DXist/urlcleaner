#!/usr/bin/env python
# coding: utf-8

"""Tests for urlcleaner"""

import logging
import os
import unittest

from urlcleaner import check_urls


class TestCheckUrls(unittest.TestCase):
    def test_ok(self):
        urls = [
            'https://twitter.com/anilkirbas',
            'https://www.twitter.com/rsk_living',
            'https://twitter.com/assaf'
        ]

        check_urls(urls)


if __name__ == '__main__':
    os.environ['PYTHONASYNCIODEBUG'] = '1'
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
