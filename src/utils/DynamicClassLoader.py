#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
动态加载类

@author: zhangzichao
@date: 2016-09-30
'''


from importlib import import_module


class DynamicClassLoader(object):

    """
    动态加载类
    @param classPath: 类路径
    """
    @staticmethod
    def load(classPath):
        try:
            dot = classPath.rindex('.')
            return getattr(import_module(classPath[:dot]), classPath[dot + 1:])
        except Exception, e:
            raise e.__class__("%s cannot be imported bacause %s" % (classPath, str(e)))
