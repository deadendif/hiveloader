#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
校验参数的合法性

@author: zhangzichao
@date: 2016-09-29
'''

import os
import logging

from src.parser import conf
from src.utils.TimeUtils import TimeUtils


logger = logging.getLogger('stdout')


def validate(params):
    if len(params) not in [8, 9, 10]:
        logger.error("Wrong params numbers: [params=%s]" % str(params[1:]))
        return False

    if params[0] == '':
        logger.error("Tag cannot be empty")
        return False

    if params[1].count('&') != params[2].count('&'):
        logger.error("Hql's number is not equal to dir name's number: [hqls=%s] [dirNames=%s]" % (
            params[1], params[2]))
        return False

    if params[1].count('&') != params[3].count('&'):
        logger.error("Hql's number is not equal to file name's number: [hqls=%s] [fileNames=%s]" % (
            params[1], params[3]))
        return False

    if not params[4].isdigit() and not params[5].isdigit():
        logger.error(
            "Param maxFileSize and serialNoWidth must be non-negative integer: [maxFileSize=%s] [serialNoWidth=%s]" % (params[4], params[5]))
        return False

    if params[6] != '':
        checkersPath = conf.get('vgopReloader', 'checkers.path')
        if not os.path.isfile(os.path.join(checkersPath, params[6])):
            logger.error("No such checker found in '%s': [checkerName=%s]" % (checkersPath, params[6]))
            return False

        if params[7] == '':
            logger.error("Checker field separator cannot be empty")
            return False

    if len(params) == 9 and params[8].upper() not in ['DAY', 'MONTH']:
        logger.error("Cycle type must be 'DAY' or 'MONTH': [type=%s]" % params[8])
        return False

    if len(params) == 10 and not TimeUtils.isComparable(params[8], params[9]):
        logger.error("Date format is invalid: [startDate=%s] [endDate=%s]" % (params[8], params[9]))
        return False

    logger.info("Params validation success")
    return True
