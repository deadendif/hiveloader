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
from src.operators.WebReloader import WebReloader
from src.utils.TimeLimitExecutor import TimeLimitExecutor

logger = logging.getLogger('stdout')


"""
校验参数的合法性
"""


def validate():
    if len(sys.argv) != 4:
        logger.error("Not enough params: [params=%s]" % str(sys.argv[1:]))
        return False

    if '%s' not in sys.argv[3]:
        logger.error("HQL format is invalid: [hql=%s]" % sys.argv[3])
        return False

    if sys.argv[1] == '':
        logger.error("Tag cannot be empty")
        return False

    logger.info("Params validation success")
    return True


"""
1. 检测近duration天内继上次回导后，是否有新tag生成(新tag包含首次生成的tag和重跑生成的tag)
   若有，则返回近duration天内新tag中最早的那个tag信息，否则脚本退出
2. 根据tag信息（tag生成时间、tag所属日期目录）从Hive中导出数据到本地，同时备份数据
3. 更新该tag此次回导操作的历史时间记录
"""


def main(tag, table, hql):
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
        reloader = WebReloader(tag=tag,
                               table=table,
                               loadPath=os.path.join(conf.get('webReloader', 'load.path'), table),
                               bakupPath=os.path.join(conf.get('webReloader', 'bakup.path'),
                                                      table, recordDay),
                               hql=hql % recordDay,
                               tagsHistoryPath=conf.get('webReloader', 'tags.history.path'),
                               operationTime=detectResult.minTagsSetTime,
                               separator=conf.get('webReloader', 'field.separator', '|'),
                               parallel=conf.getint('webReloader', 'reload.parallel'),
                               retryTimes=conf.getint('webReloader', 'retry.time'))
        if not reloader.run():
            exit(-1)
    else:
        logger.info("No need to run web reloader because of no tag detected")


if __name__ == '__main__':
    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    if validate():
        executor = TimeLimitExecutor(conf.getint('webReloader', 'run.timeout'), main,
                                     args=(sys.argv[1], sys.argv[2], sys.argv[3]))
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
