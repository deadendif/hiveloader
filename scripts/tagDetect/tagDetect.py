#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
检测tag

@author: zhangzichao
@date: 2016-10-08
'''

import sys
import logging
import logging.config

from src.parser import conf
from src.tagDetector.TagDetector import TagDetector
from src.utils.TimeLimitExecutor import TimeLimitExecutor
from validate import validate


logger = logging.getLogger('stdout')


def main(tag, historyPath, duration=None):
    """ 检测近duration天内tag是否有新的有效生成 """
    duration = conf.getint('basic', 'sync.duration') if duration is None else duration
    logger.info("Running tag detector [tag=%s] [historyPath=%s] [duration=%s] ..." % (tag, historyPath, duration))

    tagDetector = TagDetector(
        tag=tag,
        duration=duration,
        tagsSetPath=conf.get('basic', 'fs.tags.path'),
        tagsHistoryPath=historyPath,
        times=conf.getint('tagDetector', 'detect.times'),
        interval=conf.getint('tagDetector', 'detect.interval'))
    logger.info(tagDetector.detect())


def getFuncAndArgs(args):
    """ 根据参数判断执行的函数，并解析参数 """
    if (len(args)) == 3:
        args[2] = int(args[2])

    return main, tuple(args)
    

if __name__ == '__main__':
    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    params = list(sys.argv[1:])
    if validate(params):
        func, args = getFuncAndArgs(params)
        logger.info("Print func and args: [func=%s] [args=%s]" % (func.__name__, str(args)))

        executor = TimeLimitExecutor(conf.getint('tagDetector', 'run.timeout'), func, args=args)
        exitCode = executor.execute()
        if exitCode == 0:
            logger.info("Execute tag detect success")
        elif exitCode is None:
            logger.error("Execute tag detect timeout, and it has been killed")
            exit(-1)
        else:
            logger.error("Execute tag detect failed: [exitCode=%s]" % str(exitCode))
            exit(-1)
    else:
        logger.error("Params validation failed")
        exit(-1)
