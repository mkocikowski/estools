# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import logging
import collections
import json
import functools

logger = logging.getLogger(__name__)


ApiResponse = collections.namedtuple(
    'ApiResponse', ['status', 'reason', 'data', 'curl']
)


def request(fn):
    """http://stackoverflow.com/a/1594484/469997"""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        response, curl = fn(*args, **kwargs)
        api_r = ApiResponse(response.status_code, response.reason, response.text, curl)
        if api_r.status < 300:
            logger.debug(
                json.dumps({
                    'status': api_r.status,
                    'reason': api_r.reason,
                    'data': json.loads(api_r.data),
                }),
                extra={'funcNameOverride': fn.__name__},
            )
        else:
            logger.warning(
                json.dumps({
                    'status': api_r.status,
                    'reason': api_r.reason,
                    'data': json.loads(api_r.data),
                }),
                extra={'funcNameOverride': fn.__name__},
            )
        return api_r

    return wrapper

@request
def index_document(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, docid=None, data=None):

    verb = "POST"
    url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(data)s'" % locals()
    response = session.post(url, data=data, stream=False)
    return response, curl


@request
def delete_index(session=None, host='127.0.0.1', port=9200, index=None):

    verb = "DELETE"
    url = "http://%(host)s:%(port)i/%(index)s" % locals()
    curl = "curl -X%(verb)s %(url)s" % locals()
    response = session.delete(url, stream=False)
    return response, curl


@request
def create_index(session=None, host='127.0.0.1', port=9200, index=None):

    verb = "PUT"
    url = "http://%(host)s:%(port)i/%(index)s" % locals()
    data = {
        "settings": {
            "number_of_replicas": 0,
            "number_of_shards": 5
        },
    }
    data = json.dumps(data)
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(data)s'" % locals()
    response = session.put(url, data=data, stream=False)
    return response, curl


@request
def close_index(session=None, host='127.0.0.1', port=9200, index=None):

    verb = "POST"
    url = "http://%(host)s:%(port)i/%(index)s/_close" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s" % locals()
    response = session.post(url, stream=False)
    return response, curl


@request
def open_index(session=None, host='127.0.0.1', port=9200, index=None):

    verb = "POST"
    url = "http://%(host)s:%(port)i/%(index)s/_open" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s" % locals()
    response = session.post(url, stream=False)
    return response, curl


@request
def update_index(session=None, host='127.0.0.1', port=9200, index=None, data=None):

    verb = "PUT"
    url = "http://%(host)s:%(port)i/%(index)s/_settings" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(data)s'" % locals()
    response = session.put(url, data=data, stream=False)
    return response, curl


def set_ignore_malformed(session=None, host='127.0.0.1', port=9200, index=None, ignore=True):

# curl -XPOST 'localhost:9200/data/_close'
# curl -XPUT localhost:9200/data/_settings -d '{"index.mapping.ignore_malformed" : "true"}'
# curl -XPOST 'localhost:9200/data/_open'

    close_index(session=session, host=host, port=port, index=index)
    update_index(session=session, host=host, port=port, index=index, data=json.dumps({"index.mapping.ignore_malformed": ignore}))
    open_index(session=session, host=host, port=port, index=index)


@request
def get_mapping(session=None, host='127.0.0.1', port=9200, index=None, doctype=None):

    verb = "GET"
    url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s/_mapping" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s" % locals()
    response = session.get(url, stream=False)
    return response, curl


@request
def put_mapping(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, mapping=None):

    verb = "PUT"
    url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s/_mapping" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(mapping)s'" % locals()
    response = session.put(url, data=mapping, stream=False)
    return response, curl


@request
def _scan_query(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, query=None):

    verb = "GET"
    url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s/_search?search_type=scan&scroll=10m&size=50" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(query)s'" % locals()
    response = session.get(url, data=query, stream=False)
    return response, curl


@request
def _scan_scroll(session=None, host='127.0.0.1', port=9200, index=None, scroll_id=None):

    verb = "GET"
    url = "http://%(host)s:%(port)i/_search/scroll?scroll=10m" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(scroll_id)s'" % locals()
    response = session.get(url, data=scroll_id, stream=False)
    return response, curl


def scan_iterator(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, query='{"query": {"match_all": {}}}', raw=False):

    response = _scan_query(session=session, host=host, port=port, index=index, doctype=doctype, query=query)
    r_data = json.loads(response.data)
    while True:
        response = _scan_scroll(session=session, host=host, port=port, index=index, scroll_id=r_data['_scroll_id'])
        r_data = json.loads(response.data)
        if not r_data['hits']['hits']:
            break
        for hit in r_data['hits']['hits']:
            source = hit['_source']
            if not raw:
                source['_original'] = {
                    '_id': hit['_id'],
                    '_type': hit['_type'],
                    '_index': hit['_index'],
                }
            yield source
