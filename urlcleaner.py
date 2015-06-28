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

try:
    # Python <3.4.4.
    from asyncio import JoinableQueue as Queue
except ImportError:
    # Python >=3.4.4.
    from asyncio import Queue


logger = logging.getLogger(__name__)


class InvalidAttribute(Exception):
    pass


class URLStat:

    url = None
    local_clean_url = None
    remote_clean_url = None
    status = 'UNCLEANED'
    http_code = None
    exception = None

    def __init__(self, **kwargs):
        _allowed_attrs = self._allowed_attrs()

        for key, value in kwargs.items():
            if key not in _allowed_attrs:
                raise InvalidAttribute('Keyword argument %s is not allowed',
                                       key)
            setattr(self, key, value)

    def _allowed_attrs(self):
        return {key for key in self.__class__.__dict__.keys() if not
                key.startswith('_')}

    def __repr__(self):
        params = ['{}={}'.format(key, repr(getattr(self, key))) for key in
                  self._allowed_attrs()]
        return 'URLStat({})'.format(', '.join(params))


def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)


class URLCleaner:
    """Preprocess and clean Twitter and LinkedIn urls."""
    def __init__(self, urls, max_connections=30, num_workers=1,
                 max_tries=4, qsize=100, result_qsize=100, timeout=3, *,
                 loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.max_tries = max_tries
        self.q = Queue(maxsize=qsize, loop=self.loop)
        self.result_q = Queue(maxsize=result_qsize, loop=self.loop)
        self.timeout = timeout
        self.connector = aiohttp.TCPConnector(limit=max_connections,
                                              loop=self.loop)
        self.urls = urls

        self.t0 = time.time()
        self.t1 = None

    def local_clean(self, url):
        local_clean_url = url
        status = 'LOCAL_OK'
        return URLStat(url=url, local_clean_url=local_clean_url,
                       remote_clean_url=None, status=status, http_code=None,
                       exception=None)

    @asyncio.coroutine
    def remote_clean(self, urlstat):
        """Check URL by HEAD probing it."""
        tries = 0
        exception = None
        url = urlstat.local_clean_url
        while tries < self.max_tries:
            try:
                response = yield from asyncio.wait_for(
                    aiohttp.request('head', url, allow_redirects=False,
                                    connector=self.connector, loop=self.loop),
                    self.timeout, loop=self.loop)
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
            urlstat.status = 'REMOTE_ERROR'
            urlstat.exception = exception
            return urlstat

        urlstat.http_code = response.status

        if is_redirect(response):
            location = response.headers['location']
            remote_clean_url = urllib.parse.urljoin(url, location)
            urlstat.remote_clean_url = remote_clean_url
        elif response.status == 200:
            urlstat.status = 'REMOTE_OK'
        else:
            urlstat.status = 'REMOTE_INVALID'

        return urlstat

    @asyncio.coroutine
    def process_url(self, url):
        urlstat = self.local_clean(url)
        if urlstat.local_clean_url:
            urlstat = yield from self.remote_clean(urlstat)
        return urlstat

    def close(self):
        """Close resources."""
        self.connector.close()

    @asyncio.coroutine
    def save_results(self):
        """Process queue items forever."""
        while True:
            urlstat = yield from self.result_q.get()
            print(urlstat)
            self.result_q.task_done()

    @asyncio.coroutine
    def work(self):
        """Process queue items forever."""
        while True:
            url = yield from self.q.get()
            urlstat = yield from self.process_url(url)
            self.q.task_done()
            self.result_q.put_nowait(urlstat)

    @asyncio.coroutine
    def clean(self):
        """Run the cleaner until all finished."""
        try:
            consumer = asyncio.Task(self.save_results(), loop=self.loop)
            workers = [asyncio.Task(self.work(), loop=self.loop) for _ in
                       range(self.num_workers)]
            self.t0 = time.time()

            for url in self.urls:
                try:
                    self.q.put_nowait(url)
                except asyncio.QueueFull:
                    yield from self.q.join()

            yield from self.q.join()
            yield from self.result_q.join()

            self.t1 = time.time()
            logger.debug('Cleaning time %.2f seconds', self.t1 - self.t0)

            for w in workers:
                w.cancel()
            consumer.cancel()
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
