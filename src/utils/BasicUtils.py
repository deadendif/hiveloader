#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
各种基本的处理工具

@author: zhangzichao
@date: 2016-10-10
'''

import re


class BasicUtils(object):

    """
    字符串解析
    '-1,-8,-15' > [-1, -8, -15]
    '-1#-3' > [-1, -2, -3]
    """
    @staticmethod
    def parse2list(s):
        s = s.replace(' ', '')
        if re.search(r'^-\d(,-\d)*$', s):
            return [int(i) for i in s.split(',')]
        elif re.search(r'^-\d#-\d$', s):
            begin, end = [int(i) for i in s.split('#')]
            if begin <= end:
                return range(begin, end + 1)
            else:
                return range(end, begin + 1)[::-1]
        else:
            return None
