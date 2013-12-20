# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import argparse
import logging
import sys
import itertools

import requests

import estools.load.api
import estools.load.data

logger = logging.getLogger(__name__)

def args_parser():

    parser = argparse.ArgumentParser(description="Elasticsearch data loader (%s)" % (estools.__version__, ), epilog="")
    parser.add_argument('--host', type=str, action='store', default='localhost', help="es host; (%(default)s)")
    parser.add_argument('--port', type=int, action='store', default=9200, help="es port; (%(default)s)")
    parser.add_argument('--mode', type=str, action='store', choices=['line', 'file'], default='line', help="one document per line of per file; (%(default)s)")
    parser.add_argument('--wipe', action='store_true', help="if set, wipe the index before inserting data; (%(default)s)")
    parser.add_argument('index', type=str, action='store', help='name of the target ES index')
    parser.add_argument('uris', metavar='URIs', nargs="*", type=str, action='store', default=['/dev/stdin'], help="read data from URI(s); (%(default)s)")

    return parser


def main():

    args = args_parser().parse_args()
    logging.basicConfig(level=logging.INFO)

    DOCTYPE = 'docs'
    session = requests.Session()

    if args.wipe:
        response = estools.load.api.delete_index(session=session, host=args.host, port=args.port, index=args.index)

    format_f = estools.load.data.format
    documents = itertools.imap(format_f, estools.load.data.documents(URIs=args.uris, mode=args.mode))
    for n, doc in enumerate((d for d in documents if d)):
        response = estools.load.api.index_document(session=session, host=args.host, port=args.port, index=args.index, doctype=DOCTYPE, docid=None, data=doc)


if __name__ == "__main__":
    main()
    sys.exit(0)


