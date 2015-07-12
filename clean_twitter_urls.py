#!/usr/bin/env python3.4
# coding: utf-8

"""Clean Twitter urls."""

import argparse
import asyncio
import csv
import logging
import sys

from signal import signal, SIGPIPE, SIG_DFL

from urlcleaner import URLCleaner, twitter_normalizer, linkedin_normalizer

logger = logging.getLogger(__name__)
signal(SIGPIPE, SIG_DFL)


normalizer_map = {
    'twitter': twitter_normalizer,
    'linkedin': linkedin_normalizer,
}


def ioreader(ioobj):
    for line in ioobj:
        url = line.strip()
        yield url


def iowriter(ioobj):
    csvwriter = csv.writer(ioobj, delimiter='	')
    ioobj.write('	'.join(('url', 'status', 'local_clean_url',
                          'remote_clean_url', 'http_code', 'exception')))
    ioobj.write('\n')

    while True:
        urlstat = (yield)
        csvwriter.writerow((
            urlstat.url, urlstat.status, urlstat.local_clean_url,
            urlstat.remote_clean_url, urlstat.http_code, urlstat.exception
        ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', help='file with twitter urls',
                        type=argparse.FileType('r', encoding='utf-8'))
    parser.add_argument('outfile', help='output file name', nargs='?',
                        type=argparse.FileType('w+', encoding='utf-8'),
                        default=sys.stdout)
    parser.add_argument('-s', '--service',
                        help='what service urls are cleaned for',
                        type=str, choices=('twitter', 'linkedin'),
                        default='twitter')
    parser.add_argument('-w', '--workers', help='number of workers',
                        type=int, default=10)
    parser.add_argument('-c', '--max-connections',
                        help='maximum number of pool connections',
                        type=int, default=30)
    parser.add_argument('-v', '--verbose', help='verbose output',
                        action='store_true')

    arguments = parser.parse_args()

    loglevel = logging.DEBUG if arguments.verbose else logging.INFO
    logging.basicConfig(level=loglevel)

    urls = ioreader(arguments.infile)
    w = iowriter(arguments.outfile)
    w.send(None)

    def result_saver(urlstat):
        w.send(urlstat)

    event_loop = asyncio.get_event_loop()

    urlcleaner = URLCleaner(urls=urls,
                            normalizer=normalizer_map[arguments.service],
                            result_saver=result_saver,
                            max_connections=arguments.max_connections,
                            num_workers=arguments.workers, loop=event_loop)

    try:
        event_loop.run_until_complete(urlcleaner.clean())
    except KeyboardInterrupt:
        logger.info("Caught keyboard interrupt. Canceling tasks...")
        urlcleaner.cancel()
        event_loop.run_forever()
    except asyncio.futures.CancelledError:
        pass
    finally:
        event_loop.close()
