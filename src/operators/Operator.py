#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
操作的基类

@author: zhangzichao
@date: 2016-09-06
'''

import os
import shutil
import logging
import commands

from src.utils.FileUtils import FileUtils


logger = logging.getLogger('stdout')


class Operator(object):

    """
    初始化，tableList、loadPathList、bakupPathList、hqlList等长且同下标的值为一组
    """
    def __init__(self, tag, tableList, loadPathList, bakupPathList, hqlList, tagsHistoryPath,
                 operationTime, separator='|', parallel=50, retryTimes=3):
        self.tag = tag                             # tag
        self.tableList = tableList                 # 表名
        self.loadPathList = loadPathList           # 下载Hive数据的存放路径
        self.bakupPathList = bakupPathList         # Hive数据备份路径
        self.hqlList = hqlList                     # 下载数据执行的sql
        self.tagsHistoryPath = tagsHistoryPath     # 操作历史的路径
        self.operationTime = operationTime         # 此次操作的时间
        self.separator = separator                 # 文件中数据字段的分隔符
        self.parallel = parallel                   # 下载操作的并发数
        self.retryTimes = retryTimes               # 下载操作最大执行次数

    """
    顺序执行各操作
    """
    def run(self):
        for i in range(len(self.tableList)):
            if not self._run(self.tableList[i], self.loadPathList[i], self.bakupPathList[i], self.hqlList[i]):
                return False
        return True

    """
    [Overwrite] 执行单个操作
    """
    def _run(self):
        pass

    """
    [Overwrite] 检查hive进程数是否小于允许的并发数，即load操作是否允许执行
    """
    def _isAllowed(self):
        pass

    """
    [Overwrite] 执行命令从Hive上下载数据
    """
    def _load(self, table, loadPath):
        pass

    """
    备份数据
    """
    def _backup(self, loadPath, bakupPath):
        FileUtils.backup(loadPath, bakupPath)
        logger.info("Backup data success: [loadPath=%s] [bakupPath=%s]" % (loadPath, bakupPath))

    """
    更新操作历史时间
    """
    def _updateHistory(self):
        dirPath = os.path.join(self.tagsHistoryPath, str(self.tag))
        if not os.path.isdir(dirPath):
            os.makedirs(dirPath)

        newFileName = os.path.join(dirPath, self.operationTime)
        files = os.listdir(dirPath)
        if len(files) == 1:
            os.rename(os.path.join(dirPath, files[0]), newFileName)
        else:  # 异常情况，清理目录
            logger.info("Cleaning tag history path: [path=%s]" % dirPath)
            for f in files:
                path = os.path.join(dirPath, f)
                if os.path.isfile(path):
                    os.remove(path)
                    logger.info("File removed: [file=%s]" % path)
                elif os.path.isdir(path):
                    shutil.rmtree(path, True)
                    logger.info("Folder removed: [folder=%s]" % path)
            os.mknod(newFileName)
        logger.info("Update history time success: [dirPath=%s] [operationTime=%s]" % (dirPath, self.operationTime))
