# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import unittest
import logging
import time
import json

import estools.common.log as log
import estools.load.load as load
import estools.dump.dump as dump
import estools.test.test_load as test_load


# inherits the skip decorator as well as the set up and clean up from
# test_load.FunctionalTest
class FunctionalTest(test_load.FunctionalTest):


    def test_functional(self):

        tests = (
            ("estools-test test", ['{"foo": 1}', '{"foo": 2}'],),
        )

        for test in tests:
            load.run(
                params=load.args_parser().parse_args(test[0].split()),
                session=self.session,
                input_i=test[1]
            )
            time.sleep(2) # give index time to refresh
            result = dump.run(
                params=dump.args_parser().parse_args(test[0].split()),
                session=self.session
            )
            self.assertEqual(sorted([json.dumps(r) for r in result]), test[1])


if __name__ == "__main__":

    log.set_up_logging(level=logging.DEBUG)
    unittest.main()

