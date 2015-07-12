#!/usr/bin/env python

# coding: utf-8

"""Tests for urlcleaner"""

import asyncio
import logging
import os
import unittest

from urlcleaner import URLCleaner, URLStat, twitter_normalizer


class TestURLCleaner(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.addCleanup(self.loop.close)
        self.urlcleaner = None

    def test_ok(self):
        url_stat_apriori = {
            '@anilkirbas': URLStat(
                url='@anilkirbas',
                local_clean_url='https://twitter.com/anilkirbas',
                remote_clean_url='https://twitter.com/anilkirbas',
                status='REMOTE_OK',
                http_code=200,
                exception=None
            ),
            'http://twitter.com/anilkirbas': URLStat(
                url='http://twitter.com/anilkirbas',
                local_clean_url='https://twitter.com/anilkirbas',
                remote_clean_url='https://twitter.com/anilkirbas',
                status='REMOTE_OK',
                http_code=200,
                exception=None
            ),
            'https://www.twitter.com/atweed': URLStat(
                url='https://www.twitter.com/atweed',
                local_clean_url='https://twitter.com/atweed',
                remote_clean_url='https://twitter.com/atweed',
                status='REMOTE_OK',
                http_code=200,
                exception=None
            ),
        }
        self.clean(url_stat_apriori, normalizer=twitter_normalizer)

    def test_bad_data(self):
        url_stat_apriori = {
            'https://www.twitter.com/some_test_url': URLStat(
                url='https://www.twitter.com/some_test_url',
                local_clean_url='https://twitter.com/some_test_url',
                remote_clean_url=None,
                status='REMOTE_INVALID',
                http_code=404,
                exception=None
            ),
            'http://twitter.com/#!/kWhOURS': URLStat(
                url='http://twitter.com/#!/kWhOURS',
                local_clean_url='https://twitter.com/kWhOURS',
                remote_clean_url=None,
                status='REMOTE_INVALID',
                http_code=302,
                exception=None
            ),
            'https://noname.noname': URLStat(
                url='https://noname.noname',
                local_clean_url=None,
                remote_clean_url=None,
                status='LOCAL_INVALID',
                http_code=None,
                exception=None
            ),
            'https://localhost:2': URLStat(
                url='https://localhost:2',
                local_clean_url=None,
                remote_clean_url=None,
                status='LOCAL_INVALID',
                http_code=None,
                exception=None
            ),
            None: URLStat(
                url=None,
                local_clean_url=None,
                remote_clean_url=None,
                status='LOCAL_INVALID',
                http_code=None,
                exception=None
            ),
        }

        self.clean(url_stat_apriori, normalizer=twitter_normalizer,)

    def clean(self, url_stat_apriori, normalizer, **kwargs):
        num_of_successes = 0

        def assert_result(urlstat):
            nonlocal num_of_successes
            apriori = url_stat_apriori[urlstat.url]
            self.assertEqual(apriori, urlstat,
                             '{} not equal to {}'.format(apriori, urlstat))
            num_of_successes += 1 # noqa

        self.urlcleaner = URLCleaner(url_stat_apriori, normalizer=normalizer,
                                     loop=self.loop,
                                     result_saver=assert_result, **kwargs)
        self.loop.run_until_complete(self.urlcleaner.clean())

        self.assertEqual(len(url_stat_apriori), num_of_successes)


if __name__ == '__main__':
    os.environ.setdefault('PYTHONASYNCIODEBUG', '1')
    loglevel = os.environ.get('LOGLEVEL', 'DEBUG')
    logging.basicConfig(level=loglevel)
    unittest.main()
