# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import logging

logger = logging.getLogger(__name__)


def set_up_logging(verbosity=0, level=logging.WARNING):

    logging.basicConfig(
        level=level-(verbosity*10),
        format='%(levelname)s %(filename)s:%(funcName)s:%(lineno)d %(message)s'
    )

    logging.getLogger("requests").setLevel(logging.ERROR)

