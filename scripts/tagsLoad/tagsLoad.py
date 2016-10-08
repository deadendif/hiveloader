#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
从HDFS下载tags

@author: zhangzichao
@date: 2016-09-08
'''

import time
import logging
import logging.config

from src.parser import conf
from src.tagsLoader.TagsLoader import TagsLoader
from src.utils.TimeLimitExecutor import TimeLimitExecutor


logger = logging.getLogger('stdout')


"""
将HDFS上近duration天的Tag及其生成时间下载到本地
"""
def main():
    logger.info("Running tags loader ...")
    tagsLoader = TagsLoader(hdfsPath=conf.get('basic', 'hdfs.tags.path'),
                            fsPath=conf.get('basic', 'fs.tags.path'),
                            duration=conf.getint('basic', 'sync.duration'))
    if not tagsLoader.run():
        exit(-1)


if __name__ == '__main__':
    logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

    executor = TimeLimitExecutor(conf.getint('tagsLoader', 'run.timeout'), main)
    exitCode = executor.execute()
    if exitCode == 0:
        logger.info("Execute tags load success")
    elif exitCode is None:
        logger.error("Execute tags load timeout, and it has been killed")
        exit(-1)
    else:
        logger.error("Execute tags load failed: [exitCode=%s]" % str(exitCode))
        exit(-1)
