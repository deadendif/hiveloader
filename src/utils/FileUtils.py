#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
操作文件的工具类

@author: zhangzichao
@date: 2016-09-06
'''

import os
import re
import glob
import math
import time
import shutil
import commands
import fileinput


class FileUtils(object):

    """
    统计文件行数
    @param path: 文件路径或目录路径
    @return 不存在返回0，路径不存在返回-1，出错返回-2
    """
    @staticmethod
    def countFilesRow(path):
        if os.path.isfile(path):
            countCmd = "wc -l %s | awk '{print $1}'" % path
        elif os.path.isdir(path):
            countCmd = "wc -l %s/* | grep -v total | awk '{sum += $1};END {print sum}'|awk '{print $1}'" % path
        else:
            return -1

        count = commands.getstatusoutput(countCmd)[1]
        if count.isdigit():
            return int(count)
        else:
            return -2

    """
    删除目录下的隐藏文件和目录
    @param dirPath: 目录路径
    @param ignoreFolder: 是否忽略隐藏目录
    """
    @staticmethod
    def rmHiddenFile(dirPath, ignoreFolder=False):
        if not os.path.isdir(dirPath):
            return

        for path in glob.iglob('%s/.*' % dirPath):
            if os.path.isdir(path):
                if not ignoreFolder:
                    shutil.rmtree(path)
            else:
                os.remove(path)

    """
    对dirPath目录下的匹配fileNamePattern的文件添加扩展名extension
    @param dirPath: 目录路径
    @param extension: 扩展名，如 '.txt'
    @param fileNamePattern: 通配符表达式
    """
    @staticmethod
    def addExtension(dirPath, extension, fileNamePattern='*', filesOnly=True):
        if not os.path.isdir(dirPath):
            return False

        for path in glob.iglob(os.path.join(dirPath, fileNamePattern)):
            if not (filesOnly and not os.path.isfile(path)):
                os.rename(path, path + extension)
        return True

    """
    将srcDirPath目录下匹配pattern的文件备份到dstDirPath
    @param srcDirPath: 源目录
    @param dstDirPath: 备份目录
    @param pattern: 备份的文件，支持通配符
    """
    @staticmethod
    def backup(srcDirPath, dstDirPath, pattern="*"):
        if not os.path.isdir(srcDirPath):
            return

        if not os.path.isdir(dstDirPath):
            os.makedirs(dstDirPath)

        for path in glob.iglob(os.path.join(srcDirPath, pattern)):
            if os.path.isfile(path):
                shutil.copy(path, dstDirPath)


    """
    给文件每行前添加行号
    @param filePath: 文件路径
    @param separator: 分隔符
    @param start: 起始行号
    """
    @staticmethod
    def addRowIndex(filePath, separator, start=1):
        if not os.path.isfile(filePath):
            return False

        for line in fileinput.input(filePath, inplace=True):
            print str(start) + separator + line.strip('\n')
            start += 1
        return True

    """
    合并目录dirPath下匹配fileNamePattern的文件
    @param dirPath: 目录
    @param fileName: 新文件名
    @param fileNamePattern: 文件名
    """
    @staticmethod
    def merge(dirPath, fileName, fileNamePattern="*", deleteFiles=True):
        subfiles = glob.glob(os.path.join(dirPath, fileNamePattern))
        if len(subfiles) == 0:
            return True

        cmd = 'cat %s > %s' % (' '.join(subfiles), os.path.join(dirPath, fileName))
        if os.system(cmd) == 0:
            if deleteFiles:
                for sf in subfiles:
                    os.remove(sf)
            return True
        else:
            return False

    """
    切分大文件，判断服务器split高低版本，v8.16及以上版本才有--additional-suffix选项
    @param filePath: 文件路径
    @param maxFileSize: 切分后文件大小的最大值，单位:字节
    @param prefix: 文件名前缀
    @param suffix: 文件名后缀
    @param serialNoWidth: 编号位数
    """
    @staticmethod
    def split(filePath, maxFileSize, prefix='', suffix='', serialNoWidth=3):
        if not os.path.isfile(filePath):
            return False
            
        # 判断split版本
        getVersionCmd = "split --version | head -1 | awk '{print $NF}'"
        out = commands.getstatusoutput(getVersionCmd)
        lowVersion = False
        if out[0] != 0 or not re.match('^\d.\d\d$', out[1]) or out[1] < '8.16':
            lowVersion = True

        prefix = int(time.time()) if prefix == '' else prefix
        dirPath = os.path.dirname(filePath)
        fileName = os.path.basename(filePath)
        cmd = "cd %s && split -C %d -a %d %s -d '%s' '%s'" % (dirPath, maxFileSize, serialNoWidth, '' if lowVersion else "--additional-suffix '%s'" % suffix, fileName, prefix)
        if os.system(cmd) == 0:
            os.remove(filePath)
            if lowVersion and suffix:
                fileNamePattern = prefix + '[0-9]' * serialNoWidth 
                return FileUtils.addExtension(dirPath, suffix, fileNamePattern)
            return True
        return False

    """
    清空目录下的文件
    @param dirPath: 目录路径
    """
    @staticmethod
    def clean(dirPath):
        if not os.path.isdir(dirPath):
            return

        shutil.rmtree(dirPath)
        os.makedirs(dirPath)
