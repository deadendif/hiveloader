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

    def __init__(self, tag, table, loadPath, bakupPath, hql, tagsHistoryPath, operationTime, separator='|', parallel=50, retryTimes=3):
        self.tag = tag                             # tag
        self.table = table                         # 表名
        self.loadPath = loadPath                   # 下载Hive数据的存放路径
        self.bakupPath = bakupPath                 # Hive数据备份路径
        self.hql = hql                             # 下载数据执行的sql
        self.tagsHistoryPath = tagsHistoryPath     # 操作历史的路径
        self.operationTime = operationTime         # 此次操作的时间
        self.separator = separator                 # 文件中数据字段的分隔符
        self.parallel = parallel                   # 下载操作的并发数
        self.retryTimes = retryTimes               # 下载操作最大执行次数

        if not os.path.isdir(self.loadPath):
            os.makedirs(self.loadPath)

        if not os.path.isdir(self.bakupPath):
            os.makedirs(self.bakupPath)

    """
    [Overwrite] 执行
    """

    def run(self):
        pass

    """
    [Overwrite] 检查hive进程数是否小于允许的并发数，即load操作是否允许执行
    """

    def _isAllowed(self):
        pass
        # getProcessCountCmd = "jps -m|grep -v 'Jps -m'|awk '{print $3}'|grep  hive|wc -l"
        # count = int(commands.getstatusoutput(getProcessCountCmd)[1].strip())
        # logger.info("Get hive process count: [cmd=%s] [limit=%d] [count=%d]" % (getProcessCountCmd, self.parallel, count))
        # return count < self.parallel

    """
    [Overwrite] 执行命令从Hive上下载数据
    """

    def _load(self):
        pass
        # while not self.__isAllowed():
        #     time.sleep(60)

        # loadCmd = "hive -e \"set mapred.job.queue.name=etl;insert overwrite local directory '%s' ROW FORMAT DELIMITED FIELDS TERMINATED BY %s %s;\"" % (loadPath, self.separator, self.hql)
        # while self.retryTimes > 0:
        #     self.retryTimes -= 1
        #     logger.info("Executing load commnad: [retryTimes=%d][cmd=%s]" % (self.retryTimes, loadCmd))
        #     if 0 == os.system(loadCmd):
        #         logger.info("Load from hive success: [loadPath=%s] [loadRecordNum=%d]" % (self.loadPath, FileUtils.countRow(self.loadPath)))
        #         break
        # else:
        #     logger.error("Executing load commnad failed")
        #     return False
        # return True

    """
    备份数据
    """

    def _backup(self):
        FileUtils.backup(self.loadPath, self.bakupPath)
        logger.info("Backup data success: [bakupPath=%s]" % self.bakupPath)

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
