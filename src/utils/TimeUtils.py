#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
操作时间的工具类

@author: zhangzichao
@date: 2016-09-07
'''


import time
import calendar
from datetime import datetime, timedelta


class TimeUtils(object):

    """
    秒级或毫秒级时间戳转成时间
    """
    @staticmethod
    def tsp2time(tsp, fmt='%Y-%m-%d %H:%M:%S'):
        if len(str(tsp)) == 10:
            return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s))
        elif len(str(tsp)) == 13:
            s, ms = divmod(tsp, 1000)
            return '%s.%03d' % (time.strftime(fmt, time.gmtime(s)), ms)
        return None

    """
    返回时间nowDate增加delta天（或月）后的时间
    201609,-2 >> 201607; 20160922,3 >> 20160925
    """
    @staticmethod
    def timedelta(nowDate, delta=-1):
        if len(nowDate) == 8:
            return (datetime.strptime(nowDate, '%Y%m%d') + timedelta(days=delta)).strftime('%Y%m%d')
        elif len(nowDate) == 6:
            if delta <= 0:
                while delta < 0:
                    nowDate = (datetime.strptime(nowDate, '%Y%m') + timedelta(days=-1)).strftime('%Y%m')
                    delta += 1
            else:
                while delta > 0:
                    now = datetime.strptime(nowDate, '%Y%m')
                    daysInMonth = calendar.monthrange(now.year, now.month)[1]
                    nowDate = (now + timedelta(days=daysInMonth)).strftime('%Y%m')
                    delta -= 1
            return nowDate
        raise Exception("Wrong date format: [date=%s]" % nowDate)

    """
    返回字符串是否为天日期或月日期
    @param date: 日期字符串
    """
    @staticmethod
    def isDayOrMonth(date):
        if not date.isdigit() or len(date) not in [6, 8]:
            return False

        try:
            datetime.strptime(date, '%Y%m' if len(date) == 6 else '%Y%m%d')
        except Exception, e:
            return False
        else:
            return True

    """
    判断字符串是否为天日期或月日期且可比较
    """
    @staticmethod
    def isComparable(startDate, endDate):
        return len(startDate) == len(endDate) and TimeUtils.isDayOrMonth(startDate) \
                and TimeUtils.isDayOrMonth(endDate) and startDate <= endDate
