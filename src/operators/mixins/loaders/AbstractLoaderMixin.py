#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
核心下载

@author: zhangzichao
@date: 2016-09-20
'''

import os
import logging


logger = logging.getLogger('stdout')


class AbstractLoaderMixin(object):

    """
    初始化
    @param recordDate: 账单日期
    @param hqlList: 下载数据执行的hql
    @param loadPathList: 下载Hive数据的存放路径
    @param fileNameList: 下载数据文件名
    @param separator: 分隔符
    @param isAddRowIndex: 是否行首添加行号
    @param parallel: 下载操作的并发数
    @param retryTimes: 下载操作最大执行次数
    """
    def __init__(self, recordDate, hqlList, loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes):
        self.recordDate = recordDate
        self.hqlList = hqlList
        self.loadPathList = loadPathList
        self.fileNameList = fileNameList
        self.separator = separator
        self.isAddRowIndex = isAddRowIndex
        self.parallel = parallel
        self.retryTimes = retryTimes

    """
    顺序执行各子操作
    @return 是否全部执行成功
    """
    def run(self):
        for i in range(len(self.loadPathList)):
            if not os.path.isdir(self.loadPathList[i]):
                os.makedirs(self.loadPathList[i])
            if not self._run(i):
                return False
        return True

    """
    [Overwrite] 执行第i个子操作
    @param i: 下标
    @return 是否执行成功
    """
    def _run(self, i):
        return True

    """
    [Overwrite] load操作是否允许执行
    @return hive进程数是否小于允许的并发数
    """
    def _isAllowed(self):
        return True

    """
    [Overwrite] 执行命令从Hive上下载数据
    @param hql: 从Hive下载数据执行的HQL
    @param loadPath: 下载路径
    @param fileName: 数据文件名
    @return 是否下载成功
    """
    def _load(self, hql, loadPath, fileName):
        return True
