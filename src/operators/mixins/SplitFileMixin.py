#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
切分文件

@author: zhangzichao
@date: 2016-09-26
'''

import os
import logging

from src.utils.FileUtils import FileUtils


logger = logging.getLogger('stdout')


class SplitFileMixin(object):

    """
    初始化
    @param maxFileSize: 单文件大小的最大值
    @param serialNoWidth: 切分文件生成的编号的位数
    """
    def __init__(self, maxFileSize, serialNoWidth):
        self.maxFileSize = maxFileSize
        self.serialNoWidth = serialNoWidth
        self.division = '_'

    """
    切分文件
    @param filePath: 待切分文件
    """
    def _split(self, filePath):
        dirPath = os.path.dirname(filePath)
        fileName = os.path.basename(filePath)
        dot = fileName.rfind('.') if '.' in fileName else len(fileName)
        prefix, suffix = fileName[:dot] + self.division, fileName[dot:]
        return FileUtils.split(filePath, self.maxFileSize, prefix, suffix, self.serialNoWidth)
