# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/estools

import logging

logger = logging.getLogger(__name__)


class LogFormatter(logging.Formatter):
    """http://stackoverflow.com/a/7004565/469997"""
    def format(self, record):
        if hasattr(record, 'funcNameOverride'):
            record.funcName = record.funcNameOverride
        return super(LogFormatter, self).format(record)


def set_up_logging(level=logging.INFO):
    fmt = '%(levelname)s %(name)s.%(funcName)s:%(lineno)s %(message)s'
    logging.basicConfig(level=level)
    logging.getLogger().handlers[0].setFormatter(LogFormatter(fmt))
    logger.debug("set up logging")

