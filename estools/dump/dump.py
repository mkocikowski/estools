# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import argparse
import logging
import sys
import json

import requests

from estools import __version__
import estools.common.api as api
import estools.common.log as log


LOGGER = logging.getLogger(__name__)


def args_parser():

    epilog = """
Dumps documents from Elasticsearch. Writes documents to stdout, one json object per line.
"""

    parser = argparse.ArgumentParser(description="Elasticsearch data dumper (%s)" % (__version__, ), epilog=epilog)
    parser.add_argument('--verbose', '-v', action='count', default=0, help="try -v, -vv, -vvv; (-vv)")
    parser.add_argument('--schema', type=str, choices=['http', 'https'], default='http', help="(%(default)s)")
    parser.add_argument('--host', type=str, action='store', default='127.0.0.1', help="es host; (%(default)s)")
    parser.add_argument('--port', type=int, action='store', default=9200, help="es port; (%(default)s)")
    parser.add_argument('--page-size', metavar='N', type=int, action='store', default=1000, help="results per page per shard; (%(default)s)")
    parser.add_argument('index', type=str, help='name of the index')
    parser.add_argument('type', type=str, help='name of the doc type')

    return parser


def run(params=None, session=None):

    params.session = session
    return api.scan(params=params)


def main():

    args = args_parser().parse_args()
    log.set_up_logging(args.verbose)

    for doc in run(params=args, session=requests.Session()):
        print(json.dumps(doc))


if __name__ == "__main__":
    main()
    sys.exit(0)


