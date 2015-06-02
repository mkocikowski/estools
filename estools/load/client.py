# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools


import argparse
import logging
import sys
import time
import itertools
import collections
import os.path
import json

import requests

from estools import __version__
import estools.common.api as api


LOGGER = logging.getLogger(__name__)


def chunker(iterable=None, chunklen=None):
    """Collects data into fixed-length chunks.

    Does not pad the last chunk. Returns iterator.
    Example: chunker("abcde", 2) returns (('a', 'b'), ('c', 'd'), ('e'))

    """

    # for chunklen=3 generates (0,0,0,1,1,1,0,0,0,1,1,1,...)
    keys = (
        key
        for n in itertools.cycle((0, 1))
        for key in itertools.repeat(n, chunklen)
    )

    # ((a,b,c),(d,e,f),...)
    groups = itertools.groupby(
        iter(iterable),
        key=lambda v: next(keys),
    )

    return (v for _, v in groups)


def upload(params=None, records=None):

    def _fmt(l):
        return '{"index":{}}\n%s\n' % l

    t1 = time.time()
    body = ((_fmt(line), len(line)) for line in records if line)
    api.index_bulk(params=params, data="".join(body))
    LOGGER.info("uploaded %i documents in %.2fs", len(body), time.time()-t1)

#     if response['errors']:
#         LOGGER.error(response)
#         for item in response['errors']:
#             if 'error' in item:
#                 LOGGER.warn(item['error'])


def create_index(params=None, args=None):

    settings = None
    if args.index_settings_path:
        with open(_path(args.index_settings_path), "rU") as f:
            settings = json.loads(f.read())
    else:
        settings = {"settings": {}}

    if args.shards > 0:
        settings['settings']['number_of_shards'] = args.shards

    settings['settings']['number_of_replicas'] = 0
    api.create_index(params=params, settings=json.dumps(settings))


Params = collections.namedtuple(
    "Params",
    ["session", "host", "port", "index", "type",]
)

def run(args=None, session=None, input_i=None):

    def _path(s):
        return os.path.abspath(os.path.expanduser(s))

    params = Params(session, args.host, args.port, args.index, args.type,)

    if args.wipe:
        api.delete_index(params=params)

    create_index(params=params, args=args)
    api.update_setting(params=params, key="refresh_interval", value="-1")
    api.update_setting(params=params, key="number_of_replicas", value="0")
    if args.throttle:
        # https://www.elastic.co/guide/en/elasticsearch/reference/1.5/index-modules-store.html#store-throttling
        api.update_setting(params=params, key="store.throttle.type", value="all")
        api.update_setting(params=params, key="store.throttle.max_bytes_per_sec", value=args.throttle)

    try:

        if args.mapping_path:
            with open(_path(args.mapping_path), "rU") as f:
                mapping = f.read()
            api.put_mapping(params=params, mapping=mapping)

        for batch in chunker(iterable=input_i, chunklen=args.batch_size):
            upload(params=params, records=batch)

    finally:
        api.update_setting(params=params, key="store.throttle.type", value="merge")
        api.update_setting(params=params, key="refresh_interval", value="1s")
#         if args.segments > 0:
#             api.optimize_index(params=params, max_num_segments=args.segments)


def args_parser():

    epilog = """
Upload documents to ES index. Takes documents on stdin, one document per line.
"""

    parser = argparse.ArgumentParser(description="pat data loader (%s)" % (__version__, ), epilog=epilog)
    parser.add_argument('--verbose', '-v', action='count', default=0, help="try -v, -vv, -vvv; (-vv)")
    parser.add_argument('--host', type=str, action='store', default='127.0.0.1', help="es host; (%(default)s)")
    parser.add_argument('--port', type=int, action='store', default=9200, help="es port; (%(default)s)")
    parser.add_argument('--batch-size', metavar='N', type=int, action='store', default=5000, help="batch size; (%(default)s)")
    parser.add_argument('--shards', type=int, action='store', default=-1, help="number of primary shards; (%(default)s)")
    parser.add_argument('--throttle', type=str, action='store', default=None, help="limit upload to SIZE / sec; (%(default)s)")
    parser.add_argument('--mapping-path', metavar='PATH', type=str, action='store', default=None, help="path to mapping file; (%(default)s)")
    parser.add_argument('--index-settings-path', metavar='PATH', type=str, action='store', default=None, help="path to index settings file; (%(default)s)")
    parser.add_argument('--wipe', action='store_true', help="if set, wipe the index before inserting data; (%(default)s)")
    parser.add_argument('index', type=str, help='name of the index')
    parser.add_argument('type', type=str, help='name of the doc type')

    return parser


def main():

    args = args_parser().parse_args()
    logging.basicConfig(
        level=logging.WARNING-(args.verbose*10),
        format='%(levelname)s %(filename)s:%(funcName)s:%(lineno)d %(message)s'
    )
    logging.getLogger("requests").setLevel(logging.ERROR)

    run(args=args, session=requests.Session(), input_i=sys.stdin)


if __name__ == "__main__":

    main()
    sys.exit(0)


