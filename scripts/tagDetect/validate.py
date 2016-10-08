#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
校验参数的合法性

@author: zhangzichao
@date: 2016-10-08
'''

import logging


logger = logging.getLogger('stdout')


def validate(params):
    if len(params) not in [2, 3]:
        logger.error("Wrong params numbers: [params=%s]" % str(params))
        return False

    if params[0] == '':
        logger.error("Tag cannot be empty")
        return False

    if len(params) == 3 and not params[2].isdigit():
        logger.error("Duration must be digit, [duration=%s]" % params[2])
        return False

    logger.info("Params validation success")
    return True
