#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Web回导: 从Hive上下载数据，并为后序入Oracle库做准备

@author: zhangzichao
@date: 2016-09-20
'''

import logging
from src.operators.mixins import *


logger = logging.getLogger('stdout')


class WebReloader(JavaLoaderMixin, BackupMixin, RunSqlMixin, UpdateHistoryMixin):

    """
    初始化
    @param tableList
    @param hqlList
    @param loadPathList
    @param separator
    @param parallel
    @param retryTimes

    @param bakupPathList
    @param ignore

    @param connectionList
    @param sqlList

    @param tag
    @param tagsHistoryPath
    @param operationTime
    """
    def __init__(self, tableList, hqlList, loadPathList, separator, parallel, retryTimes,
                 bakupPathList, ignore,
                 connectionList, sqlList,
                 tag, tagsHistoryPath, operationTime):
        JavaLoaderMixin.__init__(self, tableList, hqlList, loadPathList, separator, parallel, retryTimes)
        BackupMixin.__init__(self, bakupPathList, ignore)
        RunSqlMixin.__init__(self, connectionList, sqlList)
        UpdateHistoryMixin.__init__(self, tag, tagsHistoryPath, operationTime)

    """
    [Overwrite AbstractLoaderMixin] 执行第i个子操作
    """
    def _run(self, i):
        # 从Hive下载数据
        if not self._load(self.tableList[i], self.loadPathList[i], self.hqlList[i]):
            return False
        # 备份数据
        self._backup(self.loadPathList[i], self.bakupPathList[i])
        # 执行sql，删除分区数据避免重复
        if not self._runSql(self.connectionList[i], self.sqlList[i]):
            return False
        # 更新操作历史
        self._updateHistory()
        return True


if __name__ == '__main__':

    import logging.config
    from src.parser import conf

    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    logger.info('Begin web reload ....')
    wr = WebReloader(tag='10000',
                     tableList=["QIXZ.ZZC_TEST_TABLE_ORA", ],
                     hqlList=['SELECT IP, OS_VERSION, IMEI, DAY_ID FROM AMAPP_SDK_PEXG_LOGIN WHERE DAY_ID = 20160903', ],
                     loadPathList=['tmp/hiveloader/LOGON/', ],
                     separator='|',
                     parallel=50,
                     retryTimes=3,
                     bakupPathList=['tmp/hiveloaderbakup/LOGON/20160903/', ],
                     ignore=("finishedfiles", ),
                     connection='qixz/qixz@EBBI',
                     sqlList=['DELETE FROM QIXZ.ZZC_TEST_TABLE_ORA WHERE DAY_ID = 20160903', ],
                     tagsHistoryPath='data/tagsHistory/',
                     operationTime='201609041200')
    wr.run()
