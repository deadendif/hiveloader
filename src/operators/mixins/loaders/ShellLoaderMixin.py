#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
通知执行Shell命令的方式从Hive下载数据 [杭州]

@author: zhangzichao
@date: 2016-09-26
'''

import os
import shutil
import logging
import commands

from src.utils.FileUtils import FileUtils
from AbstractLoaderMixin import AbstractLoaderMixin


logger = logging.getLogger('stdout')


class ShellLoaderMixin(AbstractLoaderMixin):

    """
    [Overwrite] 执行命令从Hive上下载数据
    """
    def _load(self, hql, loadPath, fileName):
        if not self._isAllowed("jps -ml | awk '{print $3}' | grep  hive | wc -l"):
            return False

        # 1. 删除对应文件，避免重复
        FileUtils.remove(loadPath, FileUtils.getNames(fileName)[0] + '*')

        # 2. 执行shell命令下载数据
        tmpLoadPath = os.path.join(loadPath, '.tmp')
        loadCmd = self.loadCmd.format(path=tmpLoadPath, separator=self.separator, hql=hql)
        remainTimes = self.retryTimes
        while remainTimes > 0:
            remainTimes -= 1
            logger.info("Executing load command: [remainTimes=%d] [cmd=%s]" % (remainTimes, loadCmd))
            if 0 == os.system(loadCmd):
                logger.info("Load from hive success: [loadPath=%s] [loadRecordNum=%d]" %
                            (tmpLoadPath, FileUtils.countFilesRow(tmpLoadPath)))
                break
        else:
            logger.error("Load data from hive failed: [cmd=%s]" % loadCmd)
            return False

        # 3. 删除无效的隐藏文件
        FileUtils.rmHiddenFile(tmpLoadPath)
        logger.info("Hidden files removed [loadPath=%s]" % tmpLoadPath)

        # 4. 合并文件
        if not FileUtils.merge(tmpLoadPath, fileName):
            logger.error("Merge load file failed [loadPath=%s] [fileName=%s]" % (tmpLoadPath, fileName))
            return False
        logger.info("Merge load file success [loadPath=%s] [fileName=%s]" % (tmpLoadPath, fileName))
        tmpFilePath = os.path.join(tmpLoadPath, fileName)

        # 5. 添加行号
        if self.isAddRowIndex:
            if FileUtils.addRowIndex(tmpFilePath, self.separator):
                logger.info("Add row index to file success [filePath=%s]" % tmpFilePath)
            else:
                logger.error("Add row index to file failed [filePath=%s]" % tmpFilePath)
                return False

        # 6. 移动到正式目录，并删除临时目录
        os.rename(tmpFilePath, os.path.join(loadPath, fileName))
        shutil.rmtree(tmpLoadPath)
        return True
