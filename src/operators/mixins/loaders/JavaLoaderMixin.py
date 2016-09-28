#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
厦门从Hive下载数据: 厦门由于Kerbose认证的权限问题，Hive数据下载暂时采用Java程序实现数据下载

@author: zhangzichao
@date: 2016-09-20
'''

import os
import logging
import commands

from src.utils.FileUtils import FileUtils
from AbstractLoaderMixin import AbstractLoaderMixin


logger = logging.getLogger('stdout')


class JavaLoaderMixin(AbstractLoaderMixin):

    """
    初始化
    TODO isAddRowIndex未使用
    """
    def __init__(self, recordDate, tableList, hqlList, loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes):
        super(JavaLoaderMixin, self).__init__(recordDate, tableList, hqlList,
                                              loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes)

    """
    [Overwrite] 检查hive进程数是否小于允许的并发数，即load操作是否允许执行
    """
    def _isAllowed(self):
        getProcessCountCmd = "jps -ml | awk '{print $2}' | grep \"hivedownload\" | wc -l"
        count = int(commands.getstatusoutput(getProcessCountCmd)[1].strip())
        logger.info("Get hive process count: [cmd=%s] [limit=%d] [count=%d]" %
                    (getProcessCountCmd, self.parallel, count))
        return count < self.parallel

    """
    [Overwrite] 执行命令从Hive上下载数据
    """
    def _load(self, hql, loadPath, table, fileName):
        while not self._isAllowed():
            time.sleep(60)

        filepath = os.path.join(loadPath, fileName)
        loadCmd = "java -jar lib/hivedownload/hivedownload-1.0-SNAPSHOT-jar-with-dependencies.jar '%s' '%s' '%s'" % (
            hql, filepath, self.separator)
        remainTimes = self.retryTimes
        while remainTimes > 0:
            remainTimes -= 1
            logger.info("Executing load command: [remainTimes=%d] [cmd=%s]" % (remainTimes, loadCmd))
            if 0 == os.system(loadCmd):
                logger.info("Load from hive success: [loadPath=%s] [loadRecordNum=%d]" %
                            (loadPath, FileUtils.countFilesRow(filepath)))
                break
        else:
            logger.error("Load data from hive failed: [cmd=%s]" % loadCmd)
            return False
        return True
