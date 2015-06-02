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


def _path(s):
    return os.path.abspath(os.path.expanduser(s))


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
    lines = [_fmt(line) for line in records if line]
    _, response = api.index_bulk(params=params, data="".join(lines))

    size_b = 0
    for l in lines:
        size_b += len(l)
    size_mb = float(size_b) / 2**20
    time_s = time.time() - t1

    LOGGER.info(
        "uploaded %i documents (%.2fMB) in %.2fs (%.2fMB/s)",
        len(lines), size_mb, time_s, size_mb / time_s,
    )

    # if there were errors with individual items, log them on DEBUG.
    # parsing the response is somewhat expensive, so do it only when
    # effective logging level is DEBUG
    if LOGGER.getEffectiveLevel() <= logging.DEBUG:
        response = response.json()
        if response['errors']:
            for item in response['items']:
                if 'error' in item.values()[0]:
                    LOGGER.debug(item)


def create_index(params=None, args=None):

    settings = None
    if args.index_settings_path:
        fp = _path(args.index_settings_path)
        with open(fp, "rU") as f:
            settings = json.loads(f.read())
        LOGGER.debug("loaded index config from %s", fp)

    else:
        settings = {"settings": {}}

    if args.shards > 0:
        settings['settings']['number_of_shards'] = args.shards

    settings['settings']['number_of_replicas'] = 0
    api.create_index(params=params, settings=json.dumps(settings))


def put_mapping(params=None, args=None):

    mapping = {}
    if args.mapping_path:
        fp = _path(args.mapping_path)
        with open(fp, "rU") as f:
            mapping = json.loads(f.read())
        LOGGER.debug("loaded mapping from %s", fp)

    if args.id_field:
        mapping['_id'] = {'path': args.id_field}
        LOGGER.debug("set mapping's '_id' to: %s", mapping['_id'])

    if mapping != {}:
        api.put_mapping(params=params, mapping=json.dumps(mapping))


Params = collections.namedtuple(
    "Params",
    ["session", "host", "port", "index", "type",]
)

def run(args=None, session=None, input_i=None):

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
        put_mapping(params=params, args=args)
        for batch in chunker(iterable=input_i, chunklen=args.batch_size):
            upload(params=params, records=batch)

    finally:
        api.update_setting(params=params, key="store.throttle.type", value="merge")
        api.update_setting(params=params, key="refresh_interval", value="1s")


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
    parser.add_argument('--id-field', type=str, action='store', default=None)
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


