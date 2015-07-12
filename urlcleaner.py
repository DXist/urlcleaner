# coding: utf-8

"""Cleaner for Twitter and LinkedIn urls."""
# requires python>=3.4.2
# tested with aiohttp==0.16.5

import asyncio
import aiohttp
import logging
import os
import re
import time

from urllib.parse import urlparse, urlunparse

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
            mine = getattr(self, attr)
            their = getattr(other, attr)
            if mine != their:
                logger.debug(
                    '%s attribute is different: %s != %s', attr, mine, their)
                return False

        return True


class URLCleaner:
    """Preprocess and clean urls."""
    def __init__(self, urls, normalizer, result_saver=print,
                 qsize=None, result_qsize=None, num_workers=1,
                 max_tries=4, timeout=3, max_connections=30, *, loop=None):
        """Async URLCleaner.

        :param normalizer: callable that takes url and returns normalized url
        or False when url is invalid or None, when url can't be validated.

        """
        self.urls = urls
        self.normalizer = normalizer
        self.result_saver = result_saver

        self.loop = loop or asyncio.get_event_loop()
        self.q = Queue(maxsize=qsize or num_workers * 10, loop=self.loop)
        self.result_q = Queue(maxsize=result_qsize or num_workers * 10,
                              loop=self.loop)

        self.num_workers = num_workers
        self.max_tries = max_tries
        self.timeout = timeout
        proxy = os.environ.get('http_proxy')
        if proxy:
            self.connector = aiohttp.ProxyConnector(proxy=proxy,
                                                    limit=max_connections,
                                                    loop=self.loop)
        else:
            self.connector = aiohttp.TCPConnector(limit=max_connections,
                                                  loop=self.loop)

        self.t0 = time.time()
        self.t1 = None
        self.clean_task = None

    def local_clean(self, url):
        local_clean_url = self.normalizer(url)
        if local_clean_url:
            status = 'LOCAL_OK'
        elif local_clean_url is False:
            status = 'LOCAL_INVALID'
            local_clean_url = None
        else:
            status = 'UNCLEANED'
        return URLStat(url=url, local_clean_url=local_clean_url,
                       remote_clean_url=None, status=status, http_code=None,
                       exception=None)

    @asyncio.coroutine
    def remote_clean(self, urlstat):
        """Check URL by HEAD probing it."""
        tries = 0
        exception = None
        url = urlstat.local_clean_url
        headers = {
            'Accept-Encoding': 'identity',
        }
        while tries < self.max_tries:
            try:
                response = yield from asyncio.wait_for(
                    aiohttp.request('head', url, allow_redirects=True,
                                    headers=headers,
                                    connector=self.connector, loop=self.loop),
                    self.timeout, loop=self.loop)
                response.close()

                if tries > 1:
                    logger.info('Try %r for %r success', tries, url)
                break

            except ValueError as error:
                # do not need to retry for these errors
                logger.info('For %r raised %s', url, error)
                tries = self.max_tries
                exception = error

            except aiohttp.HttpProcessingError as e:
                logger.error('Got http error for %r, exception %s', url, e)
                urlstat.http_code = e.code
                urlstat.status = 'REMOTE_ERROR'
                urlstat.exception = e
                return urlstat

            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                logger.info('Try %r for %r raised %s, %s', tries, url,
                            type(error), error)
                exception = error

            tries += 1
            yield from asyncio.sleep(0.1)
        else:
            # all tries failed
            logger.error('all tries for %r failed, exception %s', url,
                         exception)
            urlstat.status = 'REMOTE_ERROR'
            urlstat.exception = exception
            return urlstat

        urlstat.http_code = response.status

        if response.status == 200:
            remote_clean_url = self.normalizer(response.url)
            if remote_clean_url:
                urlstat.status = 'REMOTE_OK'
                urlstat.remote_clean_url = remote_clean_url
            elif remote_clean_url is False:
                urlstat.status = 'REMOTE_INVALID'
            else:
                # url requires authorization, can't clean
                urlstat.status = 'UNCLEANED'
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
            except StopIteration:
                self.cancel()

            except Exception as e: # noqa
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
    def _clean(self):
        try:
            self.consumer = asyncio.Task(self.save_results(), loop=self.loop)
            self.workers = [asyncio.Task(self.work(), loop=self.loop) for _ in
                            range(self.num_workers)]
            self.t0 = time.time()

            for url in self.urls:
                yield from self.q.put(url)

            yield from self.q.join()
            yield from self.result_q.join()

            self.t1 = time.time()
            logger.debug('Cleaning time %.2f seconds', self.t1 - self.t0)
            self.cancel()

        finally:
            self.close()

    def clean(self):
        """Run the cleaner until all finished."""
        self.clean_task = asyncio.async(self._clean(), loop=self.loop)
        return self.clean_task

    def cancel(self):
        self.consumer.cancel()
        for w in self.workers:
            w.cancel()

        self.clean_task.cancel()


def twitter_normalizer(url): # noqa
    scheme, netloc, path, _, _, fragment = urlparse(url)
    if scheme.lower() not in ('http', 'https', ''):
        logger.debug('Invalid scheme %s, url %s', scheme, url)
        return False

    if netloc in ('t.com', 'bit.ly'):
        # can't process short urls locally
        return url

    if netloc.lower() not in ('twitter.com', 'www.twitter.com', ''):
        return _url_for_unusual_twitter_loc(url, netloc, path)

    nickname = _nickname_from_path(path)

    if nickname:
        # we have url like http://twitter.com/nickname
        return _twitter_url_from_nickname(nickname)

    if fragment:
        # we have url like http://twitter.com/#!/nickname
        fragment_nickname_match = re.match(r'!/?([\w\-\.%]+)', fragment)
        if fragment_nickname_match:
            nickname = fragment_nickname_match.group(1)
            return _twitter_url_from_nickname(nickname)
        else:
            logger.debug('Invalid fragment %s', fragment)
            return False

    logger.debug('Invalid url %s', url)
    return False


def linkedin_normalizer(url): # noqa
    scheme, netloc, path, _, _, fragment = urlparse(url)
    if scheme.lower() not in ('http', 'https'):
        logger.debug('Invalid scheme %s, url %s', scheme, url)
        return False

    if netloc in ('lnkd.in', 'bit.ly'):
        # can't process short urls locally
        return url

    if not netloc.lower().endswith('linkedin.com'):
        if netloc.startswith('@'):
            # we have url like http://@nickname
            nickname = netloc[1:]
            return _linkedin_url_from_nickname(nickname)
        else:
            logger.debug('Invalid netloc %s, url %s', netloc, url)
            return False

    if re.match(r'(?:/profile/)', path):
        # https://www.linkedin.com/profile/... urls require authentication
        # won't clean it
        return None

    if re.match(r'(?:/pub/)?([\w\.%\-]+)', path):
        # https://www.linkedin.com/pub/... urls should be cleaned remotely
        # won't clean it
        return urlunparse(
            ('https', 'www.linkedin.com', path, '', '', ''))

    path_nickname_match = re.match(
        r'/(?:in/)?(?:@?([\w\.%\-]+))', path, re.ASCII)
    nickname = path_nickname_match.group(1) if path_nickname_match else None

    if nickname:
        # we have url like https://www.linkedin.com/nickname
        return _linkedin_url_from_nickname(nickname)
    elif path != '/':
        logger.debug('Invalid path %s', path)
        return False

    if fragment:
        # we have url like http://linkedin.com/#!/nickname
        fragment_nickname_match = re.match(r'!/?([\w\-\.%]+)', fragment)
        if fragment_nickname_match:
            nickname = fragment_nickname_match.group(1)
            return _linkedin_url_from_nickname(nickname)
        else:
            logger.debug('Invalid fragment %s', fragment)
            return False

    logger.debug('Invalid url %s', url)
    return False


def _url_for_unusual_twitter_loc(url, netloc, path):
    if netloc.startswith('@'):
        # we have url like http://@nickname
        nickname = netloc[1:]
        return _twitter_url_from_nickname(nickname)
    elif not path:
        # we have url like http://nickname
        nickname = netloc
        return _twitter_url_from_nickname(nickname)
    else:
        logger.debug('Invalid netloc %s, url %s', netloc, url)
        return False


def _twitter_url_from_nickname(nickname):
    if re.fullmatch(r'\w+', nickname, re.ASCII):
        return urlunparse(
            ('https', 'twitter.com', nickname, '', '', ''))
    else:
        logger.debug('Invalid nickname %s', nickname)
        return False


def _linkedin_url_from_nickname(nickname):
    if re.fullmatch(r'[\w%\.-]+', nickname, re.ASCII):
        return urlunparse(
            ('https', 'www.linkedin.com', 'in/' + nickname, '', '', ''))
    else:
        logger.debug('Invalid nickname %s', nickname)
        return False


def _nickname_from_path(path):
    if path.startswith('/'):
        nickname = path[1:]
    else:
        nickname = path

    if nickname.startswith('@'):
        # we have url like http://example.com/@nickname
        nickname = nickname[1:]

    return nickname
