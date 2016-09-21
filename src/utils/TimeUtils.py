#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
操作时间的工具类

@author: zhangzichao
@date: 2016-09-07
'''


import time
import datetime


class TimeUtils(object):

    """
    秒级或毫秒级时间戳转成时间
    """
    @staticmethod
    def tsp2time(tsp):
        if len(str(tsp)) == 10:
            return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s))
        elif len(str(tsp)) == 13:
            s, ms = divmod(tsp, 1000)
            return '%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s)), ms)
        return None
