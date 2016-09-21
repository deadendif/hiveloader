#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
检测tag -> 下载、备份、更新历史 -> 存储过程预处理

@author: zhangzichao
@date: 2016-09-07
'''

import os
import sys
import time
import logging
import logging.config
from datetime import datetime, timedelta

from src.parser import conf
from src.tagDetector.TagDetector import TagDetector
from src.tagDetector.TagDetectResult import TagDetectResult
from src.operators.webReloaders.WebReloader import WebReloader
from src.utils.TimeLimitExecutor import TimeLimitExecutor

logger = logging.getLogger('stdout')


"""
校验参数的合法性
"""
def validate():
    if len(sys.argv) != 5:
        logger.error("Wrong params numbers: [params=%s]" % str(sys.argv[1:]))
        return False

    if sys.argv[1] == '':
        logger.error("Tag cannot be empty")
        return False

    if sys.argv[2].count('&') != sys.argv[3].count('&'):
        logger.error("Table's number is not equal to hql's number: [params=%s]" % (sys.argv[1:]))
        return False

    if sys.argv[2].count('&') != sys.argv[4].count('&'):
        logger.error("Table's number is not equal to sql's number: [params=%s]" % (sys.argv[1:]))
        return False

    for table in sys.argv[2].split('&'):
        if ':' not in table:
            logger.error("Table format is invalid: [tables=%s]" % sys.argv[2])
            return False

    for hql in sys.argv[3].split('&'):
        if '%s' not in hql:
            logger.error("HQL format is invalid: [hqls=%s]" % sys.argv[3])
            return False

    for sql in sys.argv[4].split('&'):
        if '%s' not in sql:
            logger.error("SQL format is invalid: [sqls=%s]" % sys.argv[4])
            return False

    logger.info("Params validation success")
    return True


"""
1. 检测tag近duration天内继上次回导后，是否检测到新生成(新生成包含首次生成和重跑生成)
   若有，则返回近duration天内新生成中最旧的那个tag信息，否则脚本退出
2. 根据tag信息（tag生成时间戳、tag所属日期目录）从Hive中导出数据到本地，同时备份数据，执行SQL
3. 最后更新该tag此次回导操作的历史时间记录
"""
def main(tag, detailTableList, hqlList, sqlList):
    logger.info("Running tag detector ...")
    detector = TagDetector(tag=tag,
                           duration=conf.getint('basic', 'sync.duration'),
                           tagsSetPath=conf.get('basic', 'fs.tags.path'),
                           tagsHistoryPath=conf.get('webReloader', 'tags.history.path'),
                           times=conf.getint('tagDetector', 'detect.times'),
                           interval=conf.getint('tagDetector', 'detect.interval'))
    detectResult = detector.detect()
    logger.info('Detect result: %s' % str(detectResult))

    if detectResult.hasDetected:
        # 话单时间
        recordDay = (datetime.strptime(detectResult.minTagsSetTimeDate,
                                       '%Y%m%d') + timedelta(days=-1)).strftime('%Y%m%d')
        logger.info('Running web reloader ...')

        connectionList = [dt.split(':')[0] for dt in detailTableList]
        tableList = [dt.split(':')[1].upper() for dt in detailTableList]

        loadPath = conf.get('webReloader', 'load.path')
        loadPathList = [os.path.join(loadPath, table) for table in tableList]

        bakupPath = conf.get('webReloader', 'bakup.path')
        bakupPathList = [os.path.join(bakupPath, table, recordDay) for table in tableList]

        reloader = WebReloader(tag=tag,
                               tableList=tableList,
                               hqlList=[hql % recordDay for hql in hqlList],
                               loadPathList=loadPathList,
                               separator=conf.get('webReloader', 'field.separator', '|'),
                               parallel=conf.getint('webReloader', 'reload.parallel'),
                               retryTimes=conf.getint('webReloader', 'retry.time'),
                               bakupPathList=bakupPathList,
                               ignore=("finishedfiles", ),
                               connectionList=connectionList,
                               sqlList=[sql % recordDay for sql in sqlList],
                               tagsHistoryPath=conf.get('webReloader', 'tags.history.path'),
                               operationTime=detectResult.minTagsSetTime)
        if not reloader.run():
            exit(-1)
    else:
        logger.info("No need to run web reloader because of no tag detected")


if __name__ == '__main__':
    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    if validate():
        executor = TimeLimitExecutor(conf.getint('webReloader', 'run.timeout'), main,
                                     args=(sys.argv[1], sys.argv[2].split('&'), sys.argv[3].split('&'), sys.argv[4].split('&')))
        exitCode = executor.execute()
        if exitCode == 0:
            logger.info("Execute web reload success")
        elif exitCode is None:
            logger.error("Execute web reload timeout, and it has been killed")
            exit(-1)
        else:
            logger.error("Execute web reload failed: [exitCode=%s]" % str(exitCode))
            exit(-1)
    else:
        exit(-1)
