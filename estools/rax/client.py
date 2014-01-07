# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import argparse
import logging
import sys
import os
import json

# import requests
import pyrax



# alternately, you can have a file ~/.pyrax.cfg:
# [default]
# identity_type = rackspace
# region = DFW
# debug = True

RS_REGIONS = ['DFW', 'IAD']
RS_USERNAME = os.environ['OS_USERNAME']
RS_PASSWORD = os.environ['OS_PASSWORD']

pyrax.set_setting("identity_type", "rackspace")
# pyrax.set_setting("region", RS_REGIONs[0])
pyrax.set_http_debug(False)

import estools.common.api
import estools.common.log
import estools.load.data

logger = logging.getLogger(__name__)


def args_parser():

    parser = argparse.ArgumentParser(description="Rackspace cloudfiles client (%s)" % (estools.__version__, ), epilog="")
#     parser.add_argument('--host', type=str, action='store', default='localhost', help="es host; (%(default)s)")
#     parser.add_argument('--port', type=int, action='store', default=9200, help="es port; (%(default)s)")
#     parser.add_argument('--raw', action='store_true', help="if set, don't include _original data; (%(default)s)")
#     parser.add_argument('--doctype', action='store', type=str, default='doc', help="doctype (follows index name in es path); (%(default)s)")
#     parser.add_argument('index', type=str, action='store', help='name of the source ES index')

    return parser


def set_credentials(username=RS_USERNAME, password=RS_PASSWORD):
    pyrax.set_credentials(username, password)
#     logger.debug(pyrax.identity.__dict__)


def get_connections(regions=RS_REGIONS):
    connections = [pyrax.connect_to_cloudfiles(region=r) for r in regions]
    return connections

def get_containers(connection=None):
    for container in connection.get_all_containers():
#         yield (connection.connection.os_options['region_name'], container)
        yield container

def get_files(container=None):
    for object in container.get_objects():
        yield object




def main():

    estools.common.log.set_up_logging(level=logging.INFO)
    args = args_parser().parse_args()
    session = requests.Session()

#     hits = estools.common.api.scan_iterator(
#                 session=session,
#                 host=args.host,
#                 port=args.port,
#                 index=args.index,
#                 doctype=args.doctype,
#                 query='{"query": {"match_all": {}}}',
#                 raw=args.raw,
#     )
#
#     try:
#         for hit in hits:
#             print(json.dumps(hit))
#
#     except IOError as exc:
#         logger.debug(exc)

#     print(os.environ)

#     print(os.environ['CLOUD_REGION'])

#     pyrax.set_credentials(os.environ['OS_USERNAME'], os.environ['OS_PASSWORD'])



if __name__ == "__main__":
    main()
    sys.exit(0)


