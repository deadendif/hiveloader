#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
更新操作历史时间

@author: zhangzichao
@date: 2016-09-20
'''

import os
import logging


logger = logging.getLogger('stdout')


class UpdateHistoryMixin(object):

    """
    初始化
    @param tag: tag
    @param tagsHistoryPath: 操作历史的路径
    @param operationTime: 此次操作的时间
    """
    def __init__(self, tag, tagsHistoryPath, operationTime):
        self.tag = tag
        self.tagsHistoryPath = tagsHistoryPath
        self.operationTime = operationTime

    """
    更新操作历史时间
    """
    def _updateHistory(self):
        dirPath = os.path.join(self.tagsHistoryPath, str(self.tag))
        if not os.path.isdir(dirPath):
            os.makedirs(dirPath)

        newFileName = os.path.join(dirPath, self.operationTime)
        files = os.listdir(dirPath)
        if len(files) == 1:
            os.rename(os.path.join(dirPath, files[0]), newFileName)
        else:  # 异常情况，清理目录
            logger.info("Cleaning tag history path: [path=%s]" % dirPath)
            for f in files:
                path = os.path.join(dirPath, f)
                if os.path.isfile(path):
                    os.remove(path)
                    logger.info("File removed: [file=%s]" % path)
                elif os.path.isdir(path):
                    shutil.rmtree(path, True)
                    logger.info("Folder removed: [folder=%s]" % path)
            os.mknod(newFileName)
        logger.info("Update history time success: [dirPath=%s] [operationTime=%s]" % (dirPath, self.operationTime))
