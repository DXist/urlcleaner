#!/usr/bin/env python3.4
# coding: utf-8

"""Clean Twitter urls."""

import asyncio

from urlcleaner import URLCleaner


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
