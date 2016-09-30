#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
厦门从Hive下载数据: 厦门由于Kerbose认证的权限问题，Hive数据下载暂时采用Java程序实现数据下载

@author: zhangzichao
@date: 2016-09-20
'''

import os
import time
import logging
import commands

from src.utils.FileUtils import FileUtils
from AbstractLoaderMixin import AbstractLoaderMixin


logger = logging.getLogger('stdout')


class JavaLoaderMixin(AbstractLoaderMixin):

    """
    [Overwrite] 执行命令从Hive上下载数据
    """
    def _load(self, hql, loadPath, fileName):
        if not self._isAllowed("jps -ml | awk '{print $2}' | grep \"hivedownload\" | wc -l"):
            return False

        filePath = os.path.join(loadPath, fileName)
        loadCmd = self.loadCmd.format(hql=hql, path=filePath, separator=self.separator)
        remainTimes = self.retryTimes
        while remainTimes > 0:
            remainTimes -= 1
            logger.info("Executing load command: [remainTimes=%d] [cmd=%s]" % (remainTimes, loadCmd))
            if 0 == os.system(loadCmd):
                logger.info("Load from hive success: [loadPath=%s] [loadRecordNum=%d]" %
                            (loadPath, FileUtils.countFilesRow(filePath)))
                break
        else:
            logger.error("Load data from hive failed: [cmd=%s]" % loadCmd)
            return False
        return True
