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
        subfiles = [sf for sf in glob.glob(os.path.join(dirPath, fileNamePattern)) if os.path.isfile(sf)]
        filePath = os.path.join(dirPath, fileName)
        if len(subfiles) == 0:
            if not os.path.exists(filePath):
                os.mknod(filePath)
            return True

        cmd = 'cat %s > %s' % (' '.join(subfiles), filePath)
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
    def split(filePath, maxFileSize, prefix='', suffix='', serialNoWidth=3, serialNoFrom=1, delete=True):
        if not os.path.isfile(filePath):
            return False

        if os.path.getsize(filePath) <= maxFileSize:
            return True

        # 判断split版本
        getVersionCmd = "split --version | head -1 | awk '{print $NF}'"
        out = commands.getstatusoutput(getVersionCmd)
        splitTool = 'split'
        if out[0] != 0 or not re.match('^\d.\d\d$', out[1]) or out[1] < '8.16':
            splitTool = os.path.join(os.getcwd(), 'lib/coreutils', splitTool)
            if not os.path.isfile(splitTool):
                raise Exception("Core utils 'split' not installed. Please read lib/coreutils/README")

        prefix = int(time.time()) if prefix == '' else prefix
        dirPath = os.path.dirname(filePath)
        fileName = os.path.basename(filePath)
        
        cmd = "cd %s && %s -C %d -a %d --additional-suffix='%s' --numeric-suffixes=%d '%s' '%s'" % (
            dirPath, splitTool, maxFileSize, serialNoWidth, suffix, serialNoFrom, fileName, prefix)
        if os.system(cmd) == 0:
            if delete:
                os.remove(filePath)
            return True
        return False

    """
    删除目录下匹配fileNamePattern的文件
    @param dirPath: 目录路径
    @param fileNamePattern: 文件通配符
    @param ignoreFolder: 是否忽略目录
    """
    @staticmethod
    def remove(dirPath, fileNamePattern='*', ignoreFolder=False):
        if not os.path.isdir(dirPath):
            return False

        for path in glob.iglob(os.path.join(dirPath, fileNamePattern)):
            if os.path.isdir(path):
                if not ignoreFolder:
                    shutil.rmtree(path)
            else:
                os.remove(path)
        return True

    """
    删除目录下的隐藏文件和目录
    @param dirPath: 目录路径
    @param ignoreFolder: 是否忽略隐藏目录
    """
    @staticmethod
    def rmHiddenFile(dirPath, ignoreFolder=False):
        return FileUtils.remove(dirPath, '.*', ignoreFolder)

    """
    返回文件名中的文件主名和扩展名
    hiveloader.txt > hiveloader; hiveloader > hiveloader
    @param fileName: 文件名
    @return 文件主名，扩展名
    """
    @staticmethod
    def getNames(fileName):
        dot = fileName.rfind('.') if '.' in fileName else len(fileName)
        return fileName[:dot], fileName[dot:]
