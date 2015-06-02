# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import unittest
import logging

import estools.load.client as load


class LoadClientTest(unittest.TestCase):

    def test_chunker(self):

        chunks = load.chunker(iterable=range(5), chunklen=2)
        self.assertEqual(
            [list(c) for c in chunks],
            [[0,1],[2,3],[4]]
        )
        chunks = load.chunker(iterable=(s for s in "abcde"), chunklen=2)
        self.assertEqual(
            [list(c) for c in chunks],
            [['a','b'],['c','d'],['e']]
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

