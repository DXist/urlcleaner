# coding: utf-8

"""Cleaner for Twitter and LinkedIn urls."""
# requires python>=3.4.2
# tested with aiohttp==0.16.5

import asyncio
import aiohttp
import logging
import re
import time

from urllib.parse import urlparse, urlunparse, urljoin

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

    def __eq__(self, other):
        assert isinstance(other, URLStat)
        for attr in self._allowed_attrs():
            if getattr(self, attr) != getattr(other, attr):
                logger.debug('%s attribute is different', attr)
                return False

        return True


def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)


class URLCleaner:
    """Preprocess and clean urls."""
    def __init__(self, urls, normalizer, result_saver=print,
                 qsize=100, result_qsize=100, num_workers=1,
                 max_tries=4, timeout=3, max_connections=30, *, loop=None):
        self.urls = urls
        self.normalizer = normalizer
        self.result_saver = result_saver

        self.loop = loop or asyncio.get_event_loop()
        self.q = Queue(maxsize=qsize, loop=self.loop)
        self.result_q = Queue(maxsize=result_qsize, loop=self.loop)

        self.num_workers = num_workers
        self.max_tries = max_tries
        self.timeout = timeout
        self.connector = aiohttp.TCPConnector(limit=max_connections,
                                              loop=self.loop)

        self.t0 = time.time()
        self.t1 = None

    def local_clean(self, url):
        local_clean_url = self.normalizer(url)
        if local_clean_url:
            status = 'LOCAL_OK'
        else:
            status = 'LOCAL_INVALID'
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
            remote_clean_url = urljoin(url, location)
            urlstat.remote_clean_url = remote_clean_url
        elif response.status == 200:
            urlstat.status = 'REMOTE_OK'
            urlstat.remote_clean_url = url
        else:
            urlstat.status = 'REMOTE_INVALID'

        return urlstat

    @asyncio.coroutine
    def process_url(self, url):
        urlstat = self.local_clean(url)
        if urlstat.status == 'LOCAL_OK':
            urlstat = yield from self.remote_clean(urlstat)
        return urlstat

    def close(self):
        """Close resources."""
        self.connector.close()

    @asyncio.coroutine
    def save_results(self):
        """Save cleaned URLStat."""
        while True:
            urlstat = yield from self.result_q.get()
            try:
                self.result_saver(urlstat)
            except Exception as e:
                logger.exception(e)

            self.result_q.task_done()

    @asyncio.coroutine
    def work(self):
        """Process queue items forever."""
        while True:
            url = yield from self.q.get()
            urlstat = yield from self.process_url(url)
            self.q.task_done()
            yield from self.result_q.put(urlstat)

    @asyncio.coroutine
    def clean(self):
        """Run the cleaner until all finished."""
        try:
            consumer = asyncio.Task(self.save_results(), loop=self.loop)
            workers = [asyncio.Task(self.work(), loop=self.loop) for _ in
                       range(self.num_workers)]
            self.t0 = time.time()

            for url in self.urls:
                yield from self.q.put(url)

            yield from self.q.join()
            yield from self.result_q.join()

            self.t1 = time.time()
            logger.debug('Cleaning time %.2f seconds', self.t1 - self.t0)

            for w in workers:
                w.cancel()
            consumer.cancel()
        finally:
            self.close()


def twitter_normalizer(url):
    scheme, netloc, path, _, _, fragment = urlparse(url)
    if scheme not in ('http', 'https', ''):
        logger.debug('Invalid scheme %s, url %s', scheme, url)
        return None

    if netloc not in ('twitter.com', 'www.twitter.com', ''):
        if netloc.startswith('@'):
            # we have url like http://@nickname
            nickname = netloc[1:]
            return _twitter_url_from_nickname(nickname)
        else:
            logger.debug('Invalid netloc %s, url %s', netloc, url)
            return None

    nickname = _nickname_from_path(path)

    if nickname:
        # we have url like http://twitter.com/nickname
        return _twitter_url_from_nickname(nickname)

    if fragment:
        # we have url like http://twitter.com/#!/nickname
        if fragment.startswith('!/'):
            nickname = fragment[2:]
            return _twitter_url_from_nickname(nickname)

    logger.debug('Invalid fragment %s, url %s', fragment, url)


def _twitter_url_from_nickname(nickname):
    if re.fullmatch(r'\w+', nickname, re.ASCII):
        return urlunparse(
            ('https', 'twitter.com', nickname, '', '', ''))
    else:
        logger.debug('Invalid nickname %s', nickname)
        return None


def _nickname_from_path(path):
    if path.startswith('/'):
        nickname = path[1:]
    else:
        nickname = path

    if nickname.startswith('@'):
        # we have url like http://twitter.com/@nickname
        nickname = nickname[1:]

    return nickname
