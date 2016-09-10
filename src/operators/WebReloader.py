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

    def __init__(self, tag, tableList, loadPathList, bakupPathList, hqlList, sqlList, tagsHistoryPath,
                 operationTime, separator='|', parallel=50, retryTimes=3):
        super(WebReloader, self).__init__(tag, tableList, loadPathList, bakupPathList, hqlList, tagsHistoryPath,
                                          operationTime, separator, parallel, retryTimes)
        self.sqlList = sqlList      # Oracle中执行的sql语句

    def _run(self, i):
        # 初始化本地下载路径和存放路径
        self._init(self.loadPathList[i], self.bakupPathList[i])
        # 从Hive下载数据
        if not self._load(self.tableList[i], self.loadPathList[i], self.hqlList[i]):
            return False
        # 备份数据
        self._backup(self.loadPathList[i], self.bakupPathList[i])
        # 执行sql，删除分区数据避免重复
        if not self._prepareForSqlldr(self.sqlList[i]):
            return False
        # 更新操作历史
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

        filepath = os.path.join(loadPath, table + '.txt')
        loadCmd = "java -jar lib/hivedownload/hivedownload-1.0-SNAPSHOT-jar-with-dependencies.jar '%s' '%s' '%s'" % (
            hql, filepath, self.separator)
        while self.retryTimes > 0:
            self.retryTimes -= 1
            logger.info("Executing load command: [retryTimes=%d] [cmd=%s]" % (self.retryTimes, loadCmd))
            if 0 == os.system(loadCmd):
                logger.info("Load from hive success: [loadPath=%s] [loadRecordNum=%d]" %
                            (loadPath, FileUtils.countFileRow(filepath)))
                break
        else:
            logger.error("Load data from hive failed: [cmd=%s]" % loadCmd)
            return False
        return True

    """
    执行导入Oracle数据库前用于清空分区数据的sql，避免数据重复
    """
    def _prepareForSqlldr(self, sql):
        sqlCmd = "echo -e  \"%s; \n commit; \n exit;\" | sqlplus -S qixz/qixz@EBBI" % sql
        logger.info("Running sql command: [cmd=%s]" % sqlCmd)
        out = commands.getstatusoutput(sqlCmd)
        if out[0] == 0 and "ERROR" not in out[1].upper():
            logger.info("Run sql command success: [output=%s]" % out[1])
            return True
        else:
            logger.error("Run sql command exception: [cmd=%s] [output=%s]" % (sqlCmd, out[1]))
            return False


if __name__ == '__main__':

    import logging.config
    from src.parser import conf

    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    logger.info('Begin web reload ....')
    wr = WebReloader(tag='10000',
                     tableList=["LOGON", ],
                     loadPathList=['/tmp/hiveloader/LOGON/', ],
                     bakupPathList=['/tmp/hiveloaderbakup/LOGON/20160906/', ],
                     hqlList=['select * from zzc_test;', ],
                     tagsHistoryPath='data/tagsHistory/',
                     operationTime='201609041200',
                     separator='|',
                     parallel=50,
                     retryTimes=3)
    wr.run()
