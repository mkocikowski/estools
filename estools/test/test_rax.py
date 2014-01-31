# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import datetime
import os.path
import unittest
import json
import logging
import collections

try:
    import pyrax
    import estools.rax.client
    RS_USERNAME = os.environ['OS_USERNAME']
    RS_PASSWORD = os.environ['OS_PASSWORD']

except (ImportError, KeyError):
    pyrax = False


@unittest.skipIf(pyrax == False, "pyrax not installed or OS_USERNAME and OS_PASSWORD env variables not set")
class RaxTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        estools.rax.client.set_credentials()


    def test_list_files(self):
        for region, connection in estools.rax.client.get_connections().items():
            for container in estools.rax.client.get_containers(connection):
                for object in estools.rax.client.get_objects(container):
                    print((estools.rax.client.to_path(connection, container, object)))


    def test_get_connections(self):
        self.assertEqual(['IAD', 'DFW'], estools.rax.client.get_connections().keys())


    def test_get_lines(self):
        for line in estools.rax.client.get_lines(path='IAD/foo/sample_small.json'):
            print(line)


@unittest.skipIf(pyrax == False, "pyrax not installed or OS_USERNAME and OS_PASSWORD env variables not set")
class RaxUtilTest(unittest.TestCase):

    def test_from_path(self):

        self.assertEqual(('DFW', 'foo', 'bar/baz.json'), estools.rax.client.from_path("cf://dfw/foo/bar/baz.json"))
        self.assertEqual(('DFW', 'foo', 'bar/baz.json'), estools.rax.client.from_path("cf://DFW/foo/bar/baz.json"))
        self.assertEqual(('DFW', 'foo', 'bar/baz.json'), estools.rax.client.from_path("DFW/foo/bar/baz.json"))
        self.assertEqual(('DFW', 'foo', 'bar/baz.json'), estools.rax.client.from_path("/DFW/foo/bar/baz.json"))

        self.assertEqual(('DFW', 'foo', 'bar//baz.json'), estools.rax.client.from_path("/DFW/foo/bar//baz.json"))


    def test_chunks_to_lines(self):

        chunks = ['foo\nb', '', 'ar\nbaz\n\n']
        lines = estools.rax.client.chunks_to_lines(chunks)
        self.assertEqual(['foo', 'bar', 'baz'], list(lines))



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("swiftclient").setLevel(logging.INFO)
    unittest.main()

