#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
校验参数的合法性

@author: zhangzichao
@date: 2016-09-21
'''

import logging


logger = logging.getLogger('stdout')


def isDayOrMonth(date):
    return date.isdigit() and len(date) in [6,8]


def isComparable(startDate, endDate):
    return len(startDate) == len(endDate) and startDate <= endDate


def validate(params):
    if len(params) not in [4, 5, 6]:
        logger.error("Wrong params numbers: [params=%s]" % str(params[1:]))
        return False

    if params[0] == '':
        logger.error("Tag cannot be empty")
        return False

    if params[1].count('&') != params[2].count('&'):
        logger.error("Table's number is not equal to hql's number: [params=%s]" % (params[1:]))
        return False

    if params[1].count('&') != params[3].count('&'):
        logger.error("Table's number is not equal to sql's number: [params=%s]" % (params[1:]))
        return False

    for dirName in params[1].split('&'):
        if ':' not in dirName:
            logger.error("Table format is invalid: [dirNames=%s]" % params[1])
            return False

    for hql in params[2].split('&'):
        if '%s' not in hql:
            logger.error("HQL format is invalid: [hqls=%s]" % params[2])
            return False

    for sql in params[3].split('&'):
        if '%s' not in sql:
            logger.error("SQL format is invalid: [sqls=%s]" % params[3])
            return False

    if len(params) == 5:
        if params[4].upper() not in ['DAY', 'MONTH']:
            logger.error("Cycle type must be 'DAY' or 'MONTH': [type=%s]" % params[4])
            return False

    if len(params) == 6:
        if not isDayOrMonth(params[4]) or not isDayOrMonth(params[5]) or not isComparable(params[4], params[5]):
            logger.error("Date format is invalid: [startDate=%s] [endDate=%s]" % (params[4], params[5]))
            return False

    logger.info("Params validation success")
    return True