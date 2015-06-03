# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools


import argparse
import logging
import sys
import time
import itertools
import os.path
import json

import requests

from estools import __version__
import estools.common.api as api
import estools.common.log as log


LOGGER = logging.getLogger(__name__)


def load_json(file_name=""):
    """Load json object from specified file.

    Args:
        file_name: relative or absolute path

    Returns:
        dict or list, or, if file_name=="", None

    Raises:
        IOError: problem accessing / reading the file
        ValueError: problem parsing json

    """

    if not file_name:
        return None

    fp = os.path.abspath(os.path.expanduser(file_name))
    with open(fp, "rU") as f:
        data = json.loads(f.read())

    LOGGER.debug("loaded %s", fp)
    return data



def chunker(iterable=None, chunklen=None):
    """Collects data into fixed-length chunks.

    Returns: iterator of iterators. Does not pad the last chunk.
    Raises: TypeError on bad iterable or chunklen

    Example: chunker("abcde", 2) returns (('a', 'b'), ('c', 'd'), ('e'))

    This is better than the functools 'grouper' recipe (using izip on
    [iter(iterable)] * n) in that there is no performance penalty for
    really large batches.

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


def index(params=None, records=None):
    """Bulk index a single batch of records.

    Args:
        params:
        records: iterable of json objects

    Returns:
        (doc_count, error_count)
    """

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
    doc_count = len(lines)

    error_count = 0
    if params.count_errors:
        response = response.json()
        if response['errors']:
            for item in response['items']:
                if 'error' in item.values()[0]:
                    error_count += 1
                    LOGGER.debug(item)

    LOGGER.info(
        "uploaded %i documents (errors: %i) (%.2fMB) in %.2fs (%.2fMB/s)",
        doc_count, error_count, size_mb, time_s, size_mb / time_s,
    )

    return doc_count, error_count


def create_index(params=None, settings=None):
    """
    Args:
        params:
        settings: dict

    """

    if not settings:
        settings = {"settings": {}}

    if 'settings' not in settings:
        settings['settings'] = {}

    if params.shards > 0:
        settings['settings']['number_of_shards'] = params.shards

    settings['settings']['number_of_replicas'] = 0
    api.create_index(params=params, settings=json.dumps(settings))

    return settings


def put_mapping(params=None, mapping=None):
    """
    Args:
        params:
        mapping: dict

    """

    if not mapping:
        mapping = {}

    if params.id_field:
        mapping['_id'] = {'path': params.id_field}
        LOGGER.debug("set mapping's '_id' to: %s", mapping['_id'])

    if mapping != {}:
        api.put_mapping(params=params, mapping=json.dumps(mapping))

    return mapping


def run(params=None, session=None, input_i=None):
    """
    Args:
        params:
        session: requests session
        input_i: iterable of json objects

    Returns:
        count of indexed documents
    """

    params.session = session

    if params.wipe:
        api.delete_index(params=params)

    create_index(params=params, settings=load_json(params.index_settings_path))
    api.update_setting(params=params, key="refresh_interval", value="-1")
    api.update_setting(params=params, key="number_of_replicas", value="0")
    if params.throttle:
        # https://www.elastic.co/guide/en/elasticsearch/reference/1.5/index-modules-store.html#store-throttling
        api.update_setting(params=params, key="store.throttle.type", value="all")
        api.update_setting(params=params, key="store.throttle.max_bytes_per_sec", value=params.throttle)

    try:
        put_mapping(params=params, mapping=load_json(params.mapping_path))
        doc_count = 0
        error_count = 0
        for batch in chunker(iterable=input_i, chunklen=params.batch_size):
            dc, ec = index(params=params, records=batch)
            doc_count += dc
            error_count += ec

        return doc_count, error_count

    finally:
        api.update_setting(params=params, key="store.throttle.type", value="merge")
        api.update_setting(params=params, key="refresh_interval", value="1s")


def args_parser():

    epilog = """
Uploads documents to Elasticsearch index. Takes documents on stdin, one document per line.
"""

    parser = argparse.ArgumentParser(description="Elasticsearch data loader (%s)" % (__version__, ), epilog=epilog)
    parser.add_argument('--verbose', '-v', action='count', default=0, help="try -v, -vv, -vvv; (-vv)")
    parser.add_argument('--host', type=str, action='store', default='127.0.0.1', help="es host; (%(default)s)")
    parser.add_argument('--port', type=int, action='store', default=9200, help="es port; (%(default)s)")
    parser.add_argument('--wipe', action='store_true', help="wipe the index before inserting data; (%(default)s)")
    parser.add_argument('--batch-size', metavar='N', type=int, action='store', default=5000, help="batch size; (%(default)s)")
    parser.add_argument('--shards', type=int, action='store', default=-1, help="number of primary shards. -1 will use cluster defaults. Number of replicas is always set to 0 for duration of the load; (%(default)s)")
    parser.add_argument('--throttle', type=str, action='store', default=None, help="limit upload to SIZE / sec; this is not exact. SIZE can be '100kb', '1G', etc; default is unthrottled")
    parser.add_argument('--id-field', type=str, action='store', default=None, help="period-separated path to the document field from which doc ids are to be taken; 'foo.bar' will result in mapping: {'_id': {'path': 'foo.bar'}}")
    parser.add_argument('--count-errors', action='store_true', help="count errors in batchs; degrades speed; (%(default)s)")
    parser.add_argument('--mapping-path', metavar='PATH', type=str, action='store', default=None, help="path to mapping file; (%(default)s)")
    parser.add_argument('--index-settings-path', metavar='PATH', type=str, action='store', default=None, help="path to index settings file; (%(default)s)")
    parser.add_argument('index', type=str, help='name of the index')
    parser.add_argument('type', type=str, help='name of the doc type')

    return parser


def main():

    args = args_parser().parse_args()
    log.set_up_logging(args.verbose)

    doc_count, error_count = run(params=args, session=requests.Session(), input_i=sys.stdin)
    LOGGER.info("processed %i documents, errors: %i", doc_count, error_count)


if __name__ == "__main__":

    main()
    sys.exit(0)


