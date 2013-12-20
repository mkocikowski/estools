# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import datetime
import os.path
import unittest
import json
import logging
import collections

import estools.load.data


class LoadDataTest(unittest.TestCase):

    def test_data(self):

        try:

            tmp = estools.load.data.sys.stdin
            estools.load.data.sys.stdin = ['foo', 'bar', 'baz']

            with open("/tmp/esload_test_test_data", "w") as f: f.write("foo\nbar\nbaz\n")

            data = estools.load.data.documents(['-', '/tmp/esload_test_test_data'])
            self.assertIsInstance(data, collections.Iterable)
            data = list(data)
            self.assertEqual(6, len(data))

        finally:

            estools.load.data.sys.stdin = tmp


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

