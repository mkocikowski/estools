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
                LOGGER.warn(
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




#
#
# def set_ignore_malformed(session=None, host='127.0.0.1', port=9200, index=None, ignore=True):
#
# # curl -XPOST 'localhost:9200/data/_close'
# # curl -XPUT localhost:9200/data/_settings -d '{"index.mapping.ignore_malformed" : "true"}'
# # curl -XPOST 'localhost:9200/data/_open'
#
#     close_index(session=session, host=host, port=port, index=index)
#     update_index(session=session, host=host, port=port, index=index, data=json.dumps({"index.mapping.ignore_malformed": ignore}))
#     open_index(session=session, host=host, port=port, index=index)
#
#
# @request
# def get_mapping(session=None, host='127.0.0.1', port=9200, index=None, doctype=None):
#
#     verb = "GET"
#     url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s/_mapping" % locals()
#     curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s" % locals()
#     response = session.get(url, stream=False)
#     return response, curl
#
#
# @request
# def put_mapping(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, mapping=None):
#
#     verb = "PUT"
#     url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s/_mapping" % locals()
#     curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(mapping)s'" % locals()
#     response = session.put(url, data=mapping, stream=False)
#     return response, curl
#
#
# @request
# def _scan_query(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, query=None):
#
#     verb = "GET"
#     url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s/_search?search_type=scan&scroll=10m&size=50" % locals()
#     curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(query)s'" % locals()
#     response = session.get(url, data=query, stream=False)
#     return response, curl
#
#
# @request
# def _scan_scroll(session=None, host='127.0.0.1', port=9200, index=None, scroll_id=None):
#
#     verb = "GET"
#     url = "http://%(host)s:%(port)i/_search/scroll?scroll=10m" % locals()
#     curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(scroll_id)s'" % locals()
#     response = session.get(url, data=scroll_id, stream=False)
#     return response, curl
#
#
# def scan_iterator(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, query='{"query": {"match_all": {}}}', raw=False):
#
#     response = _scan_query(session=session, host=host, port=port, index=index, doctype=doctype, query=query)
#     r_data = json.loads(response.data)
#     while True:
#         response = _scan_scroll(session=session, host=host, port=port, index=index, scroll_id=r_data['_scroll_id'])
#         r_data = json.loads(response.data)
#         if not r_data['hits']['hits']:
#             break
#         for hit in r_data['hits']['hits']:
#             source = hit['_source']
#             if not raw:
#                 source['_original'] = {
#                     '_id': hit['_id'],
#                     '_type': hit['_type'],
#                     '_index': hit['_index'],
#                 }
#             yield source
