#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
tag检测的结果类

@author: zhangzichao
@date: 2016-09-05
'''


class TagDetectResult(object):

    """
    初始化
    @param tag: 检测的tag
    @param hasDetected: 该tag是否新生成
    @param minTagsSetTime: 该tag在tags集中继上次操作后最小的生成时间
    @param minTagsSetTimeDate: 该tag在HDFS上所属日期目录
    """
    def __init__(self, tag, hasDetected, minTagsSetTime=None, minTagsSetTimeDate=None):
        self.tag = tag
        self.hasDetected = hasDetected
        self.minTagsSetTime = minTagsSetTime
        self.minTagsSetTimeDate = minTagsSetTimeDate

    def __str__(self):
        return 'TagDetectResult: tag = %s, hasDetected = %s, minTagsSetTime = %s, minTagsSetTimeDate = %s' % \
            (self.tag, self.hasDetected, self.minTagsSetTime, self.minTagsSetTimeDate)
