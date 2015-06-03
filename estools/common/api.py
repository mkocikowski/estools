# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import logging
import json


LOGGER = logging.getLogger(__name__)


def request(ignore_codes=None):

    if not ignore_codes:
        ignore_codes = () # empty tuple

    def decorator(f):
        def wrapper(*args, **kwargs):

            url, response = f(*args, **kwargs)
            if response.status_code > 299:
                LOGGER.info(
                    "(%i) : %s %s : %s",
                    response.status_code,
                    response.request.method,
                    url,
                    response.text,
                )
                if response.status_code not in ignore_codes:
                    raise RuntimeError("status code %i returned by es call" % response.status_code)

            return url, response

        return wrapper
    return decorator


@request(ignore_codes=(404,))
def delete_index(params=None):

    url = "http://%(host)s:%(port)i/%(index)s" % vars(params)
    response = params.session.delete(url=url, stream=False)
    return url, response


@request(ignore_codes=(400,))
def create_index(params=None, settings=None):

    url = "http://%(host)s:%(port)i/%(index)s" % vars(params)
    response = params.session.put(
        url=url,
        data=settings,
        headers={'content-type': 'application/json'}
    )
    return url, response


@request()
def index_bulk(params=None, data=None):

    url = "http://%(host)s:%(port)i/%(index)s/%(type)s/_bulk" % vars(params)
    response = params.session.post(
        url=url,
        data=data,
        stream=False,
        headers={'content-type': 'application/json'}
    )
    return url, response


@request()
def put_mapping(params=None, mapping=None):

    url = "http://%(host)s:%(port)i/%(index)s/%(type)s/_mapping" % vars(params)
    response = params.session.put(
        url=url,
        data=mapping,
        stream=False,
        headers={'content-type': 'application/json'},
    )
    return url, response


@request()
def update_setting(params=None, key=None, value=None):

    query = json.dumps({"index": {key: value}})
    url = "http://%(host)s:%(port)i/%(index)s/_settings" % vars(params)
    LOGGER.debug("updating index settings: %s %s", url, query)
    response = params.session.put(
        url=url,
        data=query,
        headers={'content-type': 'application/json'},
    )
    return url, response


# @request()
# def optimize_index(params=None, max_num_segments=16):
#
#     LOGGER.info("optimizing index to: %i segments", max_num_segments)
#     locals().update(params._asdict())
#     url = "http://%(host)s:%(port)i/%(index)s/_optimize?max_num_segments=%(max_num_segments)i" % locals()
#     response = params.session.post(url)
#     return url, response
#


@request()
def _scan_query(params=None, query=None):

    url = "http://%(host)s:%(port)i/%(index)s/%(type)s/_search?search_type=scan&scroll=1m&size=%(page_size)i" % vars(params)
    response = params.session.get(url=url, data=query, stream=False)
    return url, response


@request()
def _scan_scroll(params=None, scroll_id=None):

    url = "http://%(host)s:%(port)i/_search/scroll?scroll=1m" % vars(params)
    response = params.session.get(url=url, data=scroll_id, stream=False)
    return url, response


def scan(params=None, query='{"query": {"match_all": {}}}'):
    """Execute a scan-scroll query, return results iterator."""

    url, response = _scan_query(params=params, query=query)
    data = response.json()

    while True:

        url, response = _scan_scroll(params=params, scroll_id=data['_scroll_id'])
        data = response.json()
        if not data['hits']['hits']:
            break

        for hit in data['hits']['hits']:
            yield hit['_source']

