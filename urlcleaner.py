#!/usr/bin/env python3.4
# coding: utf-8

"""Cleaner for Twitter and LinkedIn urls."""
# requires python>=3.4.2
# tested with aiohttp==0.16.5

import asyncio
import aiohttp
import logging
import time
import urllib
from collections import namedtuple

try:
    # Python <3.4.4.
    from asyncio import JoinableQueue as Queue
except ImportError:
    # Python >=3.4.4.
    from asyncio import Queue


logger = logging.getLogger(__name__)


URLStat = namedtuple('URLStat', ['url', 'next_url', 'status', 'exception'])


def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)


class URLCleaner:
    """Preprocess and clean Twitter and LinkedIn urls."""
    def __init__(self, urls, max_connections=30, num_workers=1,
                 max_tries=4, qsize=100, timeout=3, *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.max_tries = max_tries
        self.q = Queue(maxsize=qsize, loop=self.loop)
        self.timeout = timeout
        self.connector = aiohttp.TCPConnector(limit=max_connections,
                                              loop=self.loop)
        self.urls = urls

        self.t0 = time.time()
        self.t1 = None

    @asyncio.coroutine
    def probe_url(self, url):
        tries = 0
        exception = None
        while tries < self.max_tries:
            try:
                response = yield from asyncio.wait_for(
                    aiohttp.request('head', url, allow_redirects=False,
                                    connector=self.connector, loop=self.loop),
                    self.timeout, loop=self.loop)
                yield from response.release()
                response.close()

                if tries > 1:
                    logger.info('Try %r for %r success', tries, url)
                break
            except ValueError as client_error:
                # do not need to retry for these errors
                logger.info('Try %r for %r raised %s', tries, url,
                            client_error)
                tries = self.max_tries
                exception = client_error

            except aiohttp.ClientError as client_error:
                logger.info('Try %r for %r raised %s', tries, url,
                            client_error)
                exception = client_error

            tries += 1
        else:
            # all tries failed
            logger.error('%r failed', url)
            self.record_url_stat(URLStat(url=url, next_url=None, status=None,
                                         exception=exception))
            return

        if is_redirect(response):
            location = response.headers['location']
            next_url = urllib.parse.urljoin(url, location)
            self.record_url_stat(URLStat(url=url, next_url=next_url,
                                         status=response.status,
                                         exception=None))
        else:
            self.record_url_stat(URLStat(url=url, next_url=None,
                                         status=response.status,
                                         exception=None))
        return (response.status, url)

    def close(self):
        """Close resources."""
        self.connector.close()

    def record_url_stat(self, url_stat):
        print(url_stat)

    @asyncio.coroutine
    def work(self):
        """Process queue items forever."""
        while True:
            url = yield from self.q.get()
            yield from self.probe_url(url)
            self.q.task_done()

    @asyncio.coroutine
    def clean(self):
        """Run the cleaner until all finished."""
        try:
            workers = [asyncio.Task(self.work(), loop=self.loop) for _ in
                       range(self.num_workers)]
            self.t0 = time.time()

            for url in self.urls:
                try:
                    self.q.put_nowait(url)
                except asyncio.QueueFull:
                    yield from self.q.join()

            yield from self.q.join()

            self.t1 = time.time()
            for w in workers:
                w.cancel()
        finally:
            self.close()


if __name__ == '__main__':

    # with open('scoped_twitter_urls.txt') as f:
    _urls = [
        'https://twitter.com/anilkirbas',
        'https://www.twitter.com/rsk_living',
        'https://twitter.com/assaf',
    ]
    event_loop = asyncio.get_event_loop()
    urlcleaner = URLCleaner(urls=_urls, loop=event_loop)

    try:
        event_loop.run_until_complete(urlcleaner.clean())
    finally:
        event_loop.close()
