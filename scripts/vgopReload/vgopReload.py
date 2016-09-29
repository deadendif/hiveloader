#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Vgop回导

@author: zhangzichao
@date: 2016-09-28
'''

import os
import sys
import logging
import logging.config
from datetime import datetime

from src.parser import conf
from src.tagDetector.TagDetector import TagDetector
from src.tagDetector.TagDetectResult import TagDetectResult
from src.operators.fsReloaders.FsReloader import FsReloader
from src.utils.TimeLimitExecutor import TimeLimitExecutor
from src.utils.TimeUtils import TimeUtils
from validate import validate

logger = logging.getLogger('stdout')


def toRecordDate(day, dtype):
    """ 将tag所属日期转化成对应的账单日期 """

    recordDateTime = datetime.strptime(day, '%Y%m%d')
    return TimeUtils.prev(recordDateTime.strftime('%Y%m' if dtype == 'MONTH' else '%Y%m%d'))


def run(tag, hqlList, dirNameList, fileNameList, maxFileSize, serialNoWidth,
        checker, checkerFieldSeparator, dtype='DAY'):
    """ 定时执行 """

    logger.info("Running tag detector ...")
    detector = TagDetector(
        tag=tag,
        duration=conf.getint('basic', 'sync.duration'),
        tagsSetPath=conf.get('basic', 'fs.tags.path'),
        tagsHistoryPath=conf.get('vgopReloader', 'tags.history.path'),
        times=conf.getint('tagDetector', 'detect.times'),
        interval=conf.getint('tagDetector', 'detect.interval'))
    detectResult = detector.detect()
    logger.info('Detect result: %s' % str(detectResult))

    if detectResult.hasDetected:
        recordDate = toRecordDate(detectResult.minTagsSetTimeDate, dtype)

        loadPath = conf.get('vgopReloader', 'load.path')
        loadPathList = [os.path.join(loadPath, dirName) for dirName in dirNameList]

        logger.info('Running vgop reloader ... [recordDate=%s]' % recordDate)
        reloader = FsReloader(
            tag=tag,
            recordDate=recordDate,
            hqlList=[hql.replace('%s', recordDate) for hql in hqlList],
            loadPathList=loadPathList,
            fileNameList=[fn % recordDate for fn in fileNameList],
            separator=conf.get('vgopReloader', 'field.separator', '|'),
            isAddRowIndex=False,
            parallel=conf.getint('vgopReloader', 'reload.parallel'),
            retryTimes=conf.getint('vgopReloader', 'retry.time'),
            maxFileSize=maxFileSize,
            serialNoWidth=serialNoWidth,
            checkerPath=os.path.join(conf.get('vgopReloader', 'checkers.path'), checker) if checker != '' else '',
            checkerFieldSeparator=checkerFieldSeparator,
            bakupPathList=[],
            tagsHistoryPath=conf.get('vgopReloader', 'tags.history.path'),
            operationTime=detectResult.minTagsSetTime)
        if not reloader.run():
            exit(-1)
    else:
        logger.info("No need to run web reloader because of no tag detected")


def rerun(tag, hqlList, dirNameList, fileNameList, maxFileSize, serialNoWidth,
          checker, checkerFieldSeparator, startDate, endDate):
    pass


def getFuncAndArgs(args):
    """ 根据参数判断执行的函数，并解析参数 """
    for i in [1, 2, 3]:
        args[i] = args[i].split('&')

    for i in [4, 5]:
        args[i] = int(args[i])

    if len(args) == 9:
        args[8] = args[8].upper()

    argsFuncMap = {8: run, 9: run, 10: rerun}
    return argsFuncMap[len(args)], tuple(args)


if __name__ == '__main__':
    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    params = list(sys.argv[1:])
    if validate(params):
        func, args = getFuncAndArgs(params)
        logger.info("Print func and args: [func=%s] [args=%s]" % (func.__name__, str(args)))
        executor = TimeLimitExecutor(conf.getint('vgopReloader', 'run.timeout'), func, args=args)
        exitCode = executor.execute()
        if exitCode == 0:
            logger.info("Execute vgop reload success")
        elif exitCode is None:
            logger.error("Execute vgop reloader timeout, and it has been killed")
            exit(-1)
        else:
            logger.error("Execute vgop reload failed: [exitCode=%s]" % str(exitCode))
            exit(-1)
    else:
        logger.error("Params validation failed")
        exit(-1)
