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
    @param fileNameList
    @param separator
    @param isAddRowIndex
    @param parallel
    @param retryTimes

    @param bakupPathList

    @param connectionList
    @param sqlList

    @param tag
    @param tagsHistoryPath
    @param operationTime
    """
    def __init__(self, recordDate, tableList, hqlList, loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes,
                 bakupPathList,
                 connectionList, sqlList,
                 tag, tagsHistoryPath, operationTime):
        JavaLoaderMixin.__init__(self, recordDate, tableList, hqlList, loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes)
        BackupMixin.__init__(self, bakupPathList)
        RunSqlMixin.__init__(self, connectionList, sqlList)
        UpdateHistoryMixin.__init__(self, tag, tagsHistoryPath, operationTime)

    """
    [Overwrite JavaLoaderMixin] 执行第i个子操作
    """
    def _run(self, i):
        # 从Hive下载数据
        if not self._load(self.hqlList[i], self.loadPathList[i], self.tableList[i], self.fileNameList[i]):
            return False

        # 备份数据
        if not self._backup(self.loadPathList[i], self.bakupPathList[i], self.fileNameList[i]):
            return False

        # 执行sql，删除分区数据避免重复
        if not self._runSql(self.connectionList[i], self.sqlList[i]):
            return False

        # 更新操作历史，当operationTime无效时，不更新历史
        if self.operationTime not in ['', None] and not self._updateHistory():
            return False
        return True
