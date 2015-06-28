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
                 max_tries=4, qsize=100, *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.max_tries = max_tries
        self.q = Queue(maxsize=qsize, loop=self.loop)
        self.connector = aiohttp.TCPConnector(limit=max_connections, loop=loop)
        self.urls = urls

        self.t0 = time.time()
        self.t1 = None

    @asyncio.coroutine
    def probe_url(self, url):
        tries = 0
        exception = None
        while tries < self.max_tries:
            try:
                response = yield from aiohttp.request('head', url,
                                                      allow_redirects=False,
                                                      connector=self.connector,
                                                      loop=self.loop)
                content = yield from response.read()
                print(content)
                if tries > 1:
                    logger.info('Try %r for %r success', tries, url)
                break
            except aiohttp.ClientError as client_error:
                logger.info('Try %r for %r raised %s', tries, url,
                            client_error)
                exception = client_error
            tries += 1
        else:
            # all tries failed
            logger.error('%r failed after %r', url, self.max_tries)
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
            response.close()
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
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.num_workers)]
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


if __name__ == '__main__':

    # with open('scoped_twitter_urls.txt') as f:
    def filereader():
        pass
    event_loop = asyncio.get_event_loop()
    urlcleaner = URLCleaner(urls=filereader(), loop=event_loop)

    try:
        event_loop.run_until_complete(urlcleaner)
    finally:
        urlcleaner.close()
        event_loop.close()
