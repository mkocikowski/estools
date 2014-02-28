# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools


import sys

import os.path
import logging
import argparse
import sys
import urllib2
import gzip
import itertools
import string
import contextlib
import collections
import itertools
import json
import time

try:
    import estools.rax.client
except ImportError:
#     logger.warning("pyrax not installed, CloudFiles download not available")
    pass

try: 
    import estools.kafka.client
except ImportError:
#     logger.warning("kafka-python-basic not installed, kafka feed not available")
    pass

logger = logging.getLogger(__name__)


def format(document):

    try:
        f = float(document)
    except (ValueError, TypeError):
        try:
            json.loads(document)
            return document
        except (ValueError, TypeError):
            pass

    try:
        document = {'data': str(document)}
        document = json.dumps(document)
        return document

    except (ValueError, TypeError) as exc:
        logger.warning("failed to format a document into json", exc_info=True)
        return None


def documents(URIs=None, mode='line', failfast=False, follow=False):

    if mode == 'line':
        f = itertools.chain.from_iterable(_feeds(URIs, failfast=failfast, follow=follow))
        return (line.strip() for line in f)

    if mode == 'file':
        return ("".join(feed) for feed in _feeds(URIs, failfast=failfast, follow=follow))


def _lines(file_h, follow=False):

    while True:
        line = file_h.readline()
        if line:
            yield line
        else:
            if not follow:
                break
            time.sleep(0.1)
                


def _feeds(URIs, failfast=False, follow=False):

    for uri in URIs:
        uri = uri.strip().lower()

        if uri in ['/dev/stdin', '-']:
            logger.debug("reading from: stdin")
#             yield sys.stdin
#             def stdin():
#                 while True:
#                     line = sys.stdin.readline()
#                     if not line:
#                         break
#                     yield line
            yield _lines(sys.stdin, follow=follow)
            logger.debug("exhausted: stdin")

        elif uri.startswith('http://'):
            # TODO: read from the net
            logger.debug("skipping remote file: %s", uri)
            pass

        elif uri.startswith('cf://'):
            try:
                logger.debug("reading from: %s", uri)
                estools.rax.client.set_credentials()
                yield estools.rax.client.get_lines(path=uri)
                logger.debug("exhausted: %s", uri)
            except (NameError, ):
                logger.warning("rackspace cloud not supported, you must install 'pyrax' with 'pip install pyrax'")
            except (Exception, ) as exc:
                logger.error(exc)

        elif uri.startswith('kafka://'):
            try:
                logger.debug("reading from: %s", uri)
                yield estools.kafka.client.get_lines(path=uri, failfast=True)
                logger.debug("exhausted: %s", uri)
            except (NameError, ):
                logger.warning("kafka feed not supported, you must install 'kafka-python-basic', see: https://github.com/mkocikowski/kafka-python-basic")
            except (Exception, ) as exc:
                logger.error(exc)

        else:
            try:
                # http://stackoverflow.com/a/85237/469997
                path = os.path.abspath(uri)
                logger.debug("reading from: %s", path)
                with open(path, "rU") as f:
                    yield _lines(f, follow=follow)
                logger.debug("exhausted: %s", path)
            except IOError as exc:
                logger.warning(exc)

