#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
供其他模块调用，实现增量操作。针对某个tag，对比本地tags集（由TagsLoader
定时从HDFS上同步到本地）和本地操作记录,从而实现增量操作

@author: zhangzichao
@date: 2016-09-05
'''

import os
import time
import logging
from datetime import datetime, timedelta

from TagDetectResult import TagDetectResult


logger = logging.getLogger('stdout')


class TagDetector(object):

    """
    初始化
    @param tag: 待检测的tag
    @param duration: 检测tags集中最近duration天的时间
    @param tagsSetPath: 本地tags集的目录
    @param tagsHistoryPath: 本地tags的操作历史目录
    @param interval: 检测间隔
    @param times: 检测次数
    """

    def __init__(self, tag, duration, tagsSetPath, tagsHistoryPath, times, interval):
        self.__tag = tag
        self.__duration = duration
        self.__tagsSetPath = tagsSetPath
        self.__tagsHistoryPath = tagsHistoryPath
        self.__times = times
        self.__interval = interval

    """
    返回tag操作历史的时间，不存在时返回0
    """

    def __getTimeFromTagsHistory(self):
        dirPath = os.path.join(self.__tagsHistoryPath, self.__tag)
        if os.path.isdir(dirPath):
            files = os.listdir(dirPath)
            if len(files) == 1:
                if files[0].isdigit():
                    return files[0]
                else:
                    logger.warning("Wrong format file title: %s" % files[0])
            else:
                logger.warning("Files nums in %s: %d" % (dirPath, len(files)))
        else:
            logger.warning("No such directory: %s" % dirPath)
        return 0

    """
    返回tags集最近n天中大于操作历史时间historyTime的最小值，不存在满足条件的时间时，返回None
    @param historyTime: 操作历史时间
    """

    def __getTimeFromTagsSet(self, historyTime):
        basetime = datetime.now()
        minTagsSetTime = None      # tags集中最小值
        minTagsSetTimeDate = None  # 记录minTagsSetTime所在日志目录，即tag所属日期
        for i in range(self.__duration):
            date = (basetime + timedelta(days=-i)).strftime('%Y%m%d')
            dayTagsSetTime = self.__getTimeFromTagsSetDay(historyTime, date)

            if dayTagsSetTime is None:
                continue
            logger.info("Got tag [tag=%s] time of date [date=%s] from tags set: [time=%s]" %
                        (self.__tag, date, str(dayTagsSetTime)))

            if minTagsSetTime is None or dayTagsSetTime < minTagsSetTime:
                minTagsSetTime = dayTagsSetTime
                minTagsSetTimeDate = date

        return minTagsSetTime, minTagsSetTimeDate

    """
    返回tags集date日期中大于操作历史时间historyTime的最小值，不存在满足条件的时间时，返回None
    @param historyTime: 操作历史时间
    @param date: 日期
    """

    def __getTimeFromTagsSetDay(self, historyTime, date):
        try:
            filePath = os.path.join(self.__tagsSetPath, date, self.__tag)
            if os.path.isfile(filePath):
                with open(filePath, 'r') as freader:
                    tm = freader.read(12)
                    if tm.isdigit() and tm > historyTime:
                        return tm
            return None
        except Exception, e:
            logger.warning("Get tag [tag=%s] time of date [date=%s] from tags set exception, error: %s" % (
                self.__tag, date, str(e)))
            return None

    """
    检测times次，每次间隔interval时长
    """

    def detect(self):
        historyTime = self.__getTimeFromTagsHistory()
        logger.info("Get tag operation history time: %s" % str(historyTime))
        while self.__times > 0:
            self.__times -= 1
            minTagsSetTime, minTagsSetTimeDate = self.__getTimeFromTagsSet(historyTime)
            logger.info("Get minTagsSetTime = %s, minTagsSetTimeDate = %s, remain time = %s" %
                        (str(minTagsSetTime), str(minTagsSetTimeDate), str(self.__times)))
            if minTagsSetTime is None:
                time.sleep(self.__interval)
                continue
            return TagDetectResult(self.__tag, True, minTagsSetTime, minTagsSetTimeDate)
        return TagDetectResult(self.__tag, False)


if __name__ == '__main__':
    import logging.config
    from src.parser import conf

    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))
    logger = logging.getLogger('stdout')

    tagDetector = TagDetector(
        tag='10000',
        duration=conf.getint('basic', 'sync.duration'),
        tagsSetPath=conf.get('basic', 'fs.tags.path'),
        tagsHistoryPath=conf.get('webReloader', 'tags.history.path'),
        times=conf.getint('tagDetector', 'detect.times'),
        interval=conf.getint('tagDetector', 'detect.interval'))
    logger.info("Begin detect ...")
    logger.info(tagDetector.detect())
