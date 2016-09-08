#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Web回导: 从Hive上下载数据，并为后序入Oracle库做准备

@author: zhangzichao
@date: 2016-09-06
'''

import os
import logging
import commands

from src.utils.FileUtils import FileUtils
from src.operators.Operator import Operator


logger = logging.getLogger('stdout')


class WebReloader(Operator):

    def _run(self, table, loadPath, bakupPath, hql):
        self._init(loadPath, bakupPath)
        if not self._load(table, loadPath, hql):
            return False
        self._backup(loadPath, bakupPath)
        self._updateHistory()
        return True

    """
    初始化路径
    """
    def _init(self, loadPath, bakupPath):
        if not os.path.isdir(loadPath):
            os.makedirs(loadPath)

        if not os.path.isdir(bakupPath):
            os.makedirs(bakupPath)

    """
    检查是否未超过并发数限制，即load操作是否允许执行
    """
    def _isAllowed(self):
        getProcessCountCmd = "jps -ml | awk '{print $2}' | grep \"hivedownload\" | wc -l"
        count = int(commands.getstatusoutput(getProcessCountCmd)[1].strip())
        logger.info("Get hive process count: [cmd=%s] [limit=%d] [count=%d]" %
                    (getProcessCountCmd, self.parallel, count))
        return count < self.parallel

    """
    执行命令从Hive上下载数据
    """
    def _load(self, table, loadPath, hql):
        while not self._isAllowed():
            time.sleep(60)

        loadCmd = "java -jar lib/hivedownload/hivedownload-1.0-SNAPSHOT-jar-with-dependencies.jar '%s' '%s' '%s'" % (
            hql, os.path.join(loadPath, table + '.txt'), self.separator)
        while self.retryTimes > 0:
            self.retryTimes -= 1
            logger.info("Executing load commnad: [retryTimes=%d] [cmd=%s]" % (self.retryTimes, loadCmd))
            if 0 == os.system(loadCmd):
                logger.info("Load from hive success: [loadPath=%s] [loadRecordNum=%d]" %
                            (loadPath, FileUtils.countRow(loadPath)))
                break
        else:
            logger.error("Load data from hive failed: [cmd=%s]" % loadCmd)
            return False
        return True


if __name__ == '__main__':

    import logging.config
    from src.parser import conf

    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    logger.info('Begin web reload ....')
    wr = WebReloader(tag='10000',
                     tableList=["LOGON",],
                     loadPathList=['/tmp/hiveloader/LOGON/',],
                     bakupPathList=['/tmp/hiveloaderbakup/LOGON/20160906/',],
                     hqlList=['select * from zzc_test;',],
                     tagsHistoryPath='data/tagsHistory/',
                     operationTime='201609041200',
                     separator='|',
                     parallel=50,
                     retryTimes=3)
    wr.run()
