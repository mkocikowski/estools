# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import datetime
import os.path
import unittest
import json
import logging
import collections
import StringIO

import estools.load.data


class LoadDataTest(unittest.TestCase):

    def test_data(self):

        try:
            tmp = estools.load.data.sys.stdin
            estools.load.data.sys.stdin = StringIO.StringIO("foo\nbar\nbaz\n")
            with open("/tmp/esload_test_test_data", "w") as f: 
                f.write("foo\nbar\nbaz\n")
            data = estools.load.data.documents(['-', '/tmp/esload_test_test_data'])
            self.assertIsInstance(data, collections.Iterable)
            data = list(data)
            self.assertEqual(6, len(data))

        finally:
            estools.load.data.sys.stdin = tmp


    def test_format_document(self):

        self.assertRaises(TypeError, estools.load.data.format_document, None)
        self.assertRaises(TypeError, estools.load.data.format_document, False)

        self.assertEqual(estools.load.data.format_document(""), '{"data": ""}')
        self.assertEqual(estools.load.data.format_document('foo'), '{"data": "foo"}') # not a json value
        self.assertEqual(estools.load.data.format_document('"foo"'), '{"data": "foo"}') # json value
        self.assertEqual(estools.load.data.format_document('["foo", "bar"]'), '{"data": ["foo", "bar"]}')
        self.assertEqual(estools.load.data.format_document('{"foo": "bar"}'), '{"foo": "bar"}')



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

