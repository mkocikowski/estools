# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import logging
import collections

import requests

logger = logging.getLogger(__name__)


ApiResponse = collections.namedtuple(
    'ApiResponse', ['status', 'reason', 'data', 'curl']
)


def index_document(session=None, host='127.0.0.1', port=9200, index=None, doctype=None, docid=None, data=None):

    verb = "POST"
    url = "http://%(host)s:%(port)i/%(index)s/%(doctype)s" % locals()
    curl = "curl -X%(verb)s -H 'Content-type: application/json' %(url)s -d '%(data)s'" % locals()
    response = session.post(url, data=data, stream=False)

    api_r = ApiResponse(response.status_code, response.reason, response.text, curl)
    logger.info(api_r.data)
    return api_r


def delete_index(session=None, host='127.0.0.1', port=9200, index=None):

    verb = "DELETE"
    url = "http://%(host)s:%(port)i/%(index)s" % locals()
    curl = "curl -X%(verb)s %(url)s" % locals()
    response = session.delete(url, stream=False)

    api_r = ApiResponse(response.status_code, response.reason, response.text, curl)
    logger.info(api_r.data)
    return api_r
