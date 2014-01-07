# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import datetime
import os.path
import unittest
import json
import logging
import collections

import pyrax

import estools.rax.client


class RaxTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        estools.rax.client.set_credentials()


    def test_get_containers(self):
        for connection in estools.rax.client.get_connections():
            for container in estools.rax.client.get_containers(connection):
                for fn in estools.rax.client.get_files(container):
                    print((connection.connection.os_options['region_name'], container, fn))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("swiftclient").setLevel(logging.INFO)
    unittest.main()

