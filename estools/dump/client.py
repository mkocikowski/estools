# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import argparse
import logging
import sys
import json

import requests

import estools.common.api
import estools.common.log
import estools.load.data

logger = logging.getLogger(__name__)


def args_parser():

    parser = argparse.ArgumentParser(description="Elasticsearch data dumper (%s)" % (estools.__version__, ), epilog="")
    parser.add_argument('--host', type=str, action='store', default='localhost', help="es host; (%(default)s)")
    parser.add_argument('--port', type=int, action='store', default=9200, help="es port; (%(default)s)")
    parser.add_argument('--raw', action='store_true', help="if set, don't include _original data; (%(default)s)")
    parser.add_argument('--doctype', action='store', type=str, default='doc', help="doctype (follows index name in es path); (%(default)s)")
    parser.add_argument('index', type=str, action='store', help='name of the source ES index')

    return parser


def main():

    estools.common.log.set_up_logging(level=logging.INFO)
    args = args_parser().parse_args()
    session = requests.Session()

    hits = estools.common.api.scan_iterator(
                session=session,
                host=args.host,
                port=args.port,
                index=args.index,
                doctype=args.doctype,
                query='{"query": {"match_all": {}}}',
                raw=args.raw,
    )

    try:
        for hit in hits:
            print(json.dumps(hit))

    except IOError as exc:
        logger.debug(exc)



if __name__ == "__main__":
    main()
    sys.exit(0)


