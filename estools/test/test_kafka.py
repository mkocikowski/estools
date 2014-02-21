# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import logging
import unittest

try:
    import estools.kafka.client

except (ImportError, KeyError):
    estools.kafka.client = False


@unittest.skipIf(estools.kafka.client == False, "kafka-python-basic not installed")
class KafkaTest(unittest.TestCase):

    def test_from_path(self):
        hosts, topic = estools.kafka.client.from_path("kafka://192.168.44.11:9093,192.168.44.11:9094;topic01")
        self.assertEqual("192.168.44.11:9093,192.168.44.11:9094", hosts)
        self.assertEqual("topic01", topic)
        



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

