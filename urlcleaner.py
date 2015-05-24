#!/usr/bin/env python
# coding: utf-8

"""Cleaner Twitter and Linkein urls"""

import asyncio
import aiohttp

MAX_CONNECTIONS = 30

@asyncio.coroutine
def probe_url(session, url):
    r = yield from session.head(url)
    print(r.status, r.text(), url)
    return (r.status, url)


if __name__ == '__main__':
    conn = aiohttp.TCPConnector(limit=MAX_CONNECTIONS)
    session = aiohttp.ClientSession(connector=conn)

    with open('scoped_twitter_urls.txt') as f:
        loop = asyncio.get_event_loop()
        urls = [
        'http://twitter.com/anilkirbas'
        'http://www.twitter.com/rsk_living'
        'http://twitter.com/assaf'
        ]
        f = asyncio.wait([probe_url(session, line.strip()) for line in urls])
        loop.run_until_complete(f)
