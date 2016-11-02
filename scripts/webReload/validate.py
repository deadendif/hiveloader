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

    # hqls和sqls是否包含WHERE日期条件（全量/增量）必须保持一致（全有或全无）
    if len(set(['%s' in ql for ql in (params[2].split('&') + params[3].split('&'))])) != 1:
        logger.error("HQL's and SQL's format are not agree: [hqls=%s] [sqls=%s]" % (params[2], params[3]))
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
