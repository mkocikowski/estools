# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import logging

logger = logging.getLogger(__name__)


def set_up_logging(verbosity=0, silent=False, level=logging.WARNING):

    if silent:
        verbosity = 0

    logging.basicConfig(
        level=level-(verbosity*10),
        format='%(levelname)s %(filename)s:%(funcName)s:%(lineno)d %(message)s'
    )

    logging.getLogger("requests").setLevel(logging.ERROR)

    logging.captureWarnings(True)
    logging.getLogger("py.warnings").propagate = False
