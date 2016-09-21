#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
执行SQL语句

@author: zhangzichao
@date: 2016-09-20
'''

import logging
import commands


logger = logging.getLogger('stdout')


class RunSqlMixin(object):

    """
    初始化
    @param connection: sqlplus参数，<用户名>/<密码>@<服务名>
    @param sqlList: SQL语句
    """
    def __init__(self, connection, sqlList):
        self.connection = connection
        self.sqlList = sqlList

    """
    执行SQL语句
    """
    def _runSql(self, sql):
        sqlCmd = "echo -e  \"%s; \n commit; \n exit;\" | sqlplus -S %s" % (sql, self.connection)
        logger.info("Running sql command: [cmd=%s]" % sqlCmd)
        out = commands.getstatusoutput(sqlCmd)
        if out[0] == 0 and "ERROR" not in out[1].upper():
            logger.info("Run sql command success: [output=%s]" % out[1])
            return True
        else:
            logger.error("Run sql command exception: [cmd=%s] [output=%s]" % (sqlCmd, out[1]))
            return False
