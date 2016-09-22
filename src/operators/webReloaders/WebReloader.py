#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Web回导: 从Hive上下载数据，并为后序入Oracle库做准备

@author: zhangzichao
@date: 2016-09-20
'''

import logging
from src.operators.mixins import *


logger = logging.getLogger('stdout')


class WebReloader(JavaLoaderMixin, BackupMixin, RunSqlMixin, UpdateHistoryMixin):

    """
    初始化
    @param tableList
    @param hqlList
    @param loadPathList
    @param fileNamePattern
    @param separator
    @param parallel
    @param retryTimes

    @param bakupPathList

    @param connectionList
    @param sqlList

    @param tag
    @param tagsHistoryPath
    @param operationTime
    """
    def __init__(self, tableList, hqlList, loadPathList, fileNamePattern, separator, parallel, retryTimes,
                 bakupPathList,
                 connectionList, sqlList,
                 tag, tagsHistoryPath, operationTime):
        JavaLoaderMixin.__init__(self, tableList, hqlList, loadPathList, fileNamePattern, separator, parallel, retryTimes)
        BackupMixin.__init__(self, bakupPathList)
        RunSqlMixin.__init__(self, connectionList, sqlList)
        UpdateHistoryMixin.__init__(self, tag, tagsHistoryPath, operationTime)

    """
    [Overwrite AbstractLoaderMixin] 执行第i个子操作
    """
    def _run(self, i):
        # 从Hive下载数据
        if not self._load(self.tableList[i], self.loadPathList[i], self.hqlList[i]):
            return False
        # 备份数据
        self._backup(self.loadPathList[i], self.bakupPathList[i], self.fileNamePattern.replace('%s', '*'))
        # 执行sql，删除分区数据避免重复
        if not self._runSql(self.connectionList[i], self.sqlList[i]):
            return False
        # 更新操作历史
        if self.operationTime is not None:
            self._updateHistory()
        return True
