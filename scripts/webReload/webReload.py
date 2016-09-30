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
from src.utils.TimeUtils import TimeUtils
from validate import validate

logger = logging.getLogger('stdout')


def toRecordDate(day, dtype):
    """ 将tag所属日期转化成对应的账单日期 """

    recordDateTime = datetime.strptime(day, '%Y%m%d')
    return TimeUtils.prev(recordDateTime.strftime('%Y%m' if dtype == 'MONTH' else '%Y%m%d'))


def run(tag, detailTableList, hqlList, sqlList, dtype='DAY'):
    """ 定时执行 """

    logger.info("Running tag detector ...")
    detector = TagDetector(
        tag=tag,
        duration=conf.getint('basic', 'sync.duration'),
        tagsSetPath=conf.get('basic', 'fs.tags.path'),
        tagsHistoryPath=conf.get('webReloader', 'tags.history.path'),
        times=conf.getint('tagDetector', 'detect.times'),
        interval=conf.getint('tagDetector', 'detect.interval'))
    detectResult = detector.detect()
    logger.info('Detect result: %s' % str(detectResult))

    if detectResult.hasDetected:
        # 话单时间
        recordDate = toRecordDate(detectResult.minTagsSetTimeDate, dtype)

        connectionList = [dt.split(':')[0] for dt in detailTableList]
        dirNameList = [dt.split(':')[1].upper() for dt in detailTableList]

        loadPath = conf.get('webReloader', 'load.path')
        loadPathList = [os.path.join(loadPath, dirName) for dirName in dirNameList]

        fileNamePattern = conf.get('webReloader', 'file.name.pattern') % recordDate
        fileNameList = [fileNamePattern % dirName for dirName in dirNameList]

        bakupPath = conf.get('webReloader', 'bakup.path')
        bakupPathList = [os.path.join(bakupPath, dirName, recordDate) for dirName in dirNameList]

        logger.info('Running web reloader ... [recordDate=%s]' % recordDate)
        reloader = WebReloader(
            tag=tag,
            loadCmd=conf.get('coreHiveLoader', 'java.load.cmd'),
            recordDate=recordDate,
            hqlList=[hql % recordDate for hql in hqlList],
            loadPathList=loadPathList,
            fileNameList=fileNameList,
            separator=conf.get('webReloader', 'field.separator', '|'),
            isAddRowIndex=False,
            parallel=conf.getint('webReloader', 'reload.parallel'),
            retryTimes=conf.getint('webReloader', 'retry.time'),
            bakupPathList=bakupPathList,
            connectionList=connectionList,
            sqlList=[sql % recordDate for sql in sqlList],
            tagsHistoryPath=conf.get('webReloader', 'tags.history.path'),
            operationTime=detectResult.minTagsSetTime)
        if not reloader.run():
            exit(-1)
    else:
        logger.info("No need to run web reloader because of no tag detected")


def rerun(tag, detailTableList, hqlList, sqlList, startDate, endDate):
    """ 手动重跑，startDate和endDate为账单时间，天、月 """

    logger.info('Running web reloader: [startDate=%s] [endDate=%s]' % (startDate, endDate))
    while startDate <= endDate:
        logger.info("Running web reloader: [date=%s]" % startDate)
        connectionList = [dt.split(':')[0] for dt in detailTableList]
        dirNameList = [dt.split(':')[1].upper() for dt in detailTableList]

        loadPath = conf.get('webReloader', 'rerun.load.path')
        loadPathList = [os.path.join(loadPath, dirName) for dirName in dirNameList]

        fileNamePattern = conf.get('webReloader', 'file.name.pattern') % startDate
        fileNameList = [fileNamePattern % dirName for dirName in dirNameList]

        bakupPath = conf.get('webReloader', 'bakup.path')
        bakupPathList = [os.path.join(bakupPath, dirName, startDate) for dirName in dirNameList]

        reloader = WebReloader(
            tag=tag,
            loadCmd=conf.get('coreHiveLoader', 'java.load.cmd'),
            recordDate=startDate,
            hqlList=[hql % startDate for hql in hqlList],
            loadPathList=loadPathList,
            fileNameList=fileNameList,
            separator=conf.get('webReloader', 'field.separator', '|'),
            isAddRowIndex=False,
            parallel=conf.getint('webReloader', 'reload.parallel'),
            retryTimes=conf.getint('webReloader', 'retry.time'),
            bakupPathList=bakupPathList,
            connectionList=connectionList,
            sqlList=[sql % startDate for sql in sqlList],
            tagsHistoryPath=conf.get('webReloader', 'tags.history.path'),
            operationTime=None)
        if not reloader.run():
            exit(-1)
        logger.info("Run web reloader success: [date=%s]" % startDate)
        startDate = TimeUtils.next(startDate)
    logger.info("Run all web reloader success")


def getFuncAndArgs(args):
    """ 根据参数判断执行的函数，并解析参数 """

    for i in [1, 2, 3]:
        args[i] = args[i].split('&')

    if len(args) == 5:
        args[4] = args[4].upper()

    argsFuncMap = {4: run, 5: run, 6: rerun}
    return argsFuncMap[len(args)], tuple(args)


if __name__ == '__main__':
    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    params = list(sys.argv[1:])
    if validate(params):
        func, args = getFuncAndArgs(params)
        logger.info("Print func and args: [func=%s] [args=%s]" % (func.__name__, str(args)))
        executor = TimeLimitExecutor(conf.getint('webReloader', 'run.timeout'), func, args=args)
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
        logger.error("Params validation failed")
        exit(-1)
