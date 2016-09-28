#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
备份数据

@author: zhangzichao
@date: 2016-09-20
'''

import logging

from src.utils.FileUtils import FileUtils


logger = logging.getLogger('stdout')


class BackupMixin(object):

    """
    初始化
    @param bakupPath: 备份目录
    """
    def __init__(self, bakupPathList):
        self.bakupPathList = bakupPathList

    """
    将loadPath目录下匹配pattern的文件备份到bakupPath
    @param loadPath: 下载路径
    @param bakupPath: 备份目录
    @param pattern: 备份的文件，支持通配符
    """
    def _backup(self, loadPath, bakupPath, pattern='*'):
        FileUtils.backup(loadPath, bakupPath, pattern)
        logger.info("Backup data success: [loadPath=%s] [bakupPath=%s] [pattern=%s]" % (loadPath, bakupPath, pattern))
        return True
