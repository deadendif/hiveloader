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
    @param ignore: 源目录下忽略的文件，支持通配符
    """
    def __init__(self, bakupPathList, ignore=()):
        self.bakupPathList = bakupPathList
        self.ignore = ignore

    def _backup(self, loadPath, bakupPath):
        FileUtils.backup(loadPath, bakupPath, ignore=self.ignore)
        logger.info("Backup data success: [loadPath=%s] [bakupPath=%s]" % (loadPath, bakupPath))
