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

import estools.rax.client

logger = logging.getLogger(__name__)


def format(document):

    try:
        json.loads(document)
        return document

    except ValueError:
        pass

    try:
        document = {'data': str(document)}
        document = json.dumps(document)
        return document

    except (ValueError, TypError) as exc:
        logger.warning("failed to format a document into json", exc_info=True)
        return None


def documents(URIs=None, mode='line'):

    if mode == 'line':
        f = itertools.chain.from_iterable(_feeds(URIs))
        return (line.strip() for line in f)

    if mode == 'file':
        return ("".join(feed) for feed in _feeds(URIs))


def _feeds(URIs):

    for uri in URIs:
        uri = uri.strip().lower()

        if uri in ['/dev/stdin', '-']:
            logger.debug("reading from: stdin")
            yield sys.stdin
            logger.debug("exhausted: stdin")

        elif uri.startswith('http://'):
            # TODO: read from the net
            logger.debug("skipping remote file: %s", uri)
            pass

        elif uri.startswith('cf://'):
            logger.debug("reading from: %s", uri)
            estools.rax.client.set_credentials()
            yield estools.rax.client.get_lines(path=uri)
            logger.debug("exhausted: %s", uri)

        else:
            try:
                # http://stackoverflow.com/a/85237/469997
                path = os.path.abspath(uri)
                logger.debug("reading from: %s", path)
                with open(path) as f:
                    yield f
                logger.debug("exhausted: %s", path)
            except IOError as exc:
                logger.warning(exc)

