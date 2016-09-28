#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
本地回导：从Hive上下载数据到本地

@auther: zhangzichao
@date: 2016-09-26
'''

import os
import logging
from src.operators.mixins import *


logger = logging.getLogger('stdout')


class FsReloader(ShellLoaderMixin, SplitFileMixin, RunCheckerMixin, BackupMixin, UpdateHistoryMixin):

    """
    初始化
    @param recordDate
    @param hqlList
    @param loadPathList
    @param fileNameList
    @param isAddRowIndex
    @param separator
    @param parallel
    @param retryTimes

    @param maxFileSize
    @param serialNoWidth

    @param checkerPath
    @param checkerFieldSeparator

    @param bakupPathList

    @param tag
    @param tagsHistoryPath
    @param operationTime
    """
    def __init__(self, recordDate, hqlList, loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes,
                 maxFileSize, serialNoWidth,
                 checkerPath, checkerFieldSeparator,
                 bakupPathList,
                 tag, tagsHistoryPath, operationTime):
        ShellLoaderMixin.__init__(self, recordDate, hqlList, loadPathList,
                                  fileNameList, separator, isAddRowIndex, parallel, retryTimes)
        SplitFileMixin.__init__(self, maxFileSize, serialNoWidth)
        RunCheckerMixin.__init__(self, checkerPath, checkerFieldSeparator)
        BackupMixin.__init__(self, bakupPathList)
        UpdateHistoryMixin.__init__(self, tag, tagsHistoryPath, operationTime)

    """
    [Overwrite ShellLoaderMixin] 执行第i个子操作
    """
    def _run(self, i):
        # 从Hive上下载数据
        if not self._load(self.hqlList[i], self.loadPathList[i], self.fileNameList[i]):
            return False

        # 切分文件，当maxFileSize小于等于0时，不切分文件
        if self.maxFileSize > 0 and not self._split(os.path.join(self.loadPathList[i], self.fileNameList[i])):
            return False

        # 生成校验文件, 当checkerPath无效时，不生成校验文件
        checkFileName = self._createCheckFileName(self.fileNameList[i])
        if self.checkerPath not in ['', None] and not self._check(self.loadPathList[i], checkFileName, self.recordDate):
            return False

        # 备份，当bakupPathList无效时，不备份文件
        if self.bakupPathList not in [[], None] and not self._backup(self.loadPathList[i], self.bakupPathList[i]):
            return False

        # 更新操作历史，当operationTime无效时，不更新历史
        if self.operationTime not in ['', None] and not self._updateHistory():
            return False
        return True

    """
    根据文件名生成对应的校验文件名
    @param fileName: 文件名
    """
    def _createCheckFileName(self, fileName):
        dot = fileName.rfind('.') if '.' in fileName else len(fileName)
        return fileName[:dot] + '.verf'
