#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
操作文件的工具类

@author: zhangzichao
@date: 2016-09-06
'''

import os
import shutil
import commands


class FileUtils(object):

    """
    统计文件或目录的行数（统计目录时不统计隐藏文件）
    @return -1 不存在, -2 执行出错
    """
    @staticmethod
    def countRow(path):
        if not os.path.isdir(path) and not os.path.isfile(path):
            return -1

        param = (path + '/*') if os.path.isdir(path) else path
        countCmd = "wc -l %s | grep -v total | awk '{sum += $1};END {print sum}'|awk '{print $1}'" % param
        out = commands.getstatusoutput(countCmd)
        if 'No' in out[1].strip():
            return -2
        return int(out[1].strip())

    """
    删除目录下的隐藏文件
    """
    @staticmethod
    def rmHiddenFile(dirPath):
        if not os.path.isdir(dirPath):
            return

        rmCmd = "rm %s/.*"
        return os.system(rmCmd) == 0

    """
    对目录下的文件添加扩展名
    @param dirPath: 目录路径
    @param extension: 扩展名
    """
    @staticmethod
    def addExtension(dirPath, extension):
        if not os.path.isdir(dirPath):
            return

        files = os.listdir(dirPath)
        for fl in files:
            path = os.path.join(dirPath, fl)
            if os.path.isfile(path):
                os.rename(path, path + "." + extension)

    """
    备份目录
    @param srcDirPath: 源目录
    @param dstDirPath: 备份目录
    """
    @staticmethod
    def backup(srcDirPath, dstDirPath):
        if not os.path.isdir(srcDirPath):
            return

        shutil.rmtree(dstDirPath)
        shutil.copytree(srcDirPath, dstDirPath)
