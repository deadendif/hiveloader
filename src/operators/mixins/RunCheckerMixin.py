#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
生成校验文件

@author: zhangzichao
@date: 2016-09-27
'''

import logging
import commands


logger = logging.getLogger('stdout')


class RunCheckerMixin(object):

    """
    初始化
    @param checkerPath: 检验脚本路径
    @param checkerFieldSeparator: 校验文件中字段分隔符
    """
    def __init__(self, checkerPath, checkerFieldSeparator):
        self.checkerPath = checkerPath
        self.checkerFieldSeparator = checkerFieldSeparator

    """
    执行校验脚本
    @param loadPath: 数据文件所在路径
    @param recordDate: 数据文件的账单时间
    """
    def _check(self, loadPath, checkFileName, recordDate):
        cmd = "bash %s '%s' '%s' '%s' '%s'" % (self.checkerPath, loadPath, checkFileName,
                                               self.checkerFieldSeparator, recordDate)
        out = commands.getstatusoutput(cmd)
        if out[0] != 0:
            logger.error("Run checker failed [checkerPath=%s] [loadPath=%s], error: %s" %
                         (self.checkerPath, loadPath, str(out[1])))
            return False
        logger.info("Run checker success [checkerPath=%s] [loadPath=%s] [checkFileName=%s]" % (
            self.checkerPath, loadPath, checkFileName))
        return True
