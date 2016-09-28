#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
通知执行Shell命令的方式从Hive下载数据 [杭州]

@author: zhangzichao
@date: 2016-09-26
'''

import os
import logging
import commands

from src.utils.FileUtils import FileUtils
from AbstractLoaderMixin import AbstractLoaderMixin


logger = logging.getLogger('stdout')


class ShellLoaderMixin(AbstractLoaderMixin):

    """
    初始化
    """
    def __init__(self, recordDate, tableList, hqlList, loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes):
        super(ShellLoaderMixin, self).__init__(recordDate, tableList, hqlList,
                                               loadPathList, fileNameList, separator, isAddRowIndex, parallel, retryTimes)

    """
    [Overwrite] 检查hive进程数是否小于允许的并发数，即load操作是否允许执行
    """
    def _isAllowed(self):
        getProcessCountCmd = "jps -ml | awk '{print $3}' | grep  hive | wc -l"
        count = int(commands.getstatusoutput(getProcessCountCmd)[1].strip())
        logger.info("Get hive process count: [cmd=%s] [limit=%d] [count=%d]" %
                    (getProcessCountCmd, self.parallel, count))
        return count < self.parallel

    """
    [Overwrite] 执行命令从Hive上下载数据
    """
    def _load(self, hql, loadPath, table, fileName=None):
        while not self._isAllowed():
            time.sleep(60)

        # 1. 下载前清空本地目录
        FileUtils.clean(loadPath)
        logger.info("Load path [loadPath=%s] cleaned" % loadPath)

        # 2. 执行shell命令下载数据
        loadCmd = "hive -e \"insert overwrite local directory '%s' ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' %s\"" % (
            loadPath, self.separator, hql)
        remainTimes = self.retryTimes
        while remainTimes > 0:
            remainTimes -= 1
            logger.info("Executing load command: [remainTimes=%d] [cmd=%s]" % (remainTimes, loadCmd))
            if 0 == os.system(loadCmd):
                logger.info("Load from hive success: [loadPath=%s] [loadRecordNum=%d]" %
                            (loadPath, FileUtils.countFilesRow(loadPath)))
                break
        else:
            logger.error("Load data from hive failed: [cmd=%s]" % loadCmd)
            return False
        # 3. 删除无效的隐藏文件
        FileUtils.rmHiddenFile(loadPath)
        logger.info("Hidden files removed [loadPath=%s]" % loadPath)
        # 4. 合并文件
        if FileUtils.merge(loadPath, fileName):
            logger.info("Merge load file success [loadPath=%s] [fileName=%s]" % (loadPath, fileName))
            filePath = os.path.join(loadPath, fileName)
            if self.isAddRowIndex:
                if FileUtils.addRowIndex(filePath, self.separator):
                    logger.info("Add row index to file success [filePath=%s]" % filePath)
                else:
                    logger.error("Add row index to file failed [filePath=%s]" % filePath)
                    return False
            else:
                logger.info("No need to add row index to file")
            return True
        else:
            logger.error("Merge load file failed [loadPath=%s] [fileName=%s]" % (loadPath, fileName))
            return False
