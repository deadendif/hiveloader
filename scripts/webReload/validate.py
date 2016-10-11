#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
校验参数的合法性

@author: zhangzichao
@date: 2016-09-21
'''

import re
import logging

from src.utils.TimeUtils import TimeUtils


logger = logging.getLogger('stdout')


def validate(params):
    if len(params) not in [6, ]:
        logger.error("Wrong params numbers: [params=%s]" % str(params))
        return False

    if params[0] == '':
        logger.error("Tag cannot be empty")
        return False

    if params[1].count('&') != params[2].count('&'):
        logger.error("Table's number is not equal to hql's number: [params=%s]" % str(params))
        return False

    if params[1].count('&') != params[3].count('&'):
        logger.error("Table's number is not equal to sql's number: [params=%s]" % str(params))
        return False

    for table in params[1].split('&'):
        if ':' not in table:
            logger.error("Table format is invalid: [tables=%s]" % params[1])
            return False

    for hql in params[2].split('&'):
        if '%s' not in hql:
            logger.error("HQL format is invalid: [hqls=%s]" % params[2])
            return False

    for sql in params[3].split('&'):
        if '%s' not in sql:
            logger.error("SQL format is invalid: [sqls=%s]" % params[3])
            return False

    if params[4].upper() in ['DAY', 'MONTH']:
        # 匹配两种格式: -1,-8,-15 和 -1#-3
        if re.search(r'^-\d+#-\d+$|^-\d+(,-\d+)*$', params[5].replace(' ', '')) is None:
            logger.error("Date delta list is invalid: [deltaList=%s]" % params[5])
            return False
    elif not TimeUtils.isComparable(params[4], params[5]):
        logger.error("Date format is invalid: [startDate=%s] [endDate=%s]" % (params[4], params[5]))
        return False

    logger.info("Params validation success")
    return True