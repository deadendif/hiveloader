#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
下载某段时间范围HDFS上的tag到本地

@author: zhangzichao
@date: 2016-09-02
'''

import os
import logging
import commands
from datetime import datetime, timedelta

logger = logging.getLogger('stdout')


class TagsLoader(object):

    """
    初始化
    @param hdfsPath: tag在HDFS上的路径
    @param fsPath: 本地存放路径
    @param duration: 时间范围，单位：天
    """
    def __init__(self, hdfsPath, fsPath, duration):
        self.hdfsPath = hdfsPath
        self.fsPath = fsPath
        self.duration = duration
        self.cmd = "hadoop fs -ls %s 2> /dev/null | awk '{print $6, $7, $8}'"

    """
    执行shell命令获取basetime前duration天的tag，并写入文件
    @param basetime: 时间区间的最后一天
    """
    def run(self, basetime=None):
        basetime = datetime.now() if basetime is None else basetime
        for i in range(self.duration):
            date = (basetime + timedelta(days=-i)).strftime('%Y/%m/%d')
            rdir = os.path.join(self.hdfsPath, date)
            logger.info("Executing command to loading tags: [cmd=%s]" % (self.cmd % rdir))
            out = commands.getstatusoutput(self.cmd % rdir)
            logger.info("Command output: %s" % str(out))
            if out[0] == 0:
                self.__write(date.replace('/', ''), out[1].split('\n'))
            else:
                log.error("Load tags from HDFS exception, error: %s" % str(out[1]))

    """
    将shell命令的输出写入本地文件
    @param date: tag生成的日期
    @param lines: shell命令的输出内容
    """
    def __write(self, date, lines):
        ldir = os.path.join(self.fsPath, date)
        if not os.path.isdir(ldir):
            os.makedirs(ldir)

        for line in lines:
            if '/user' not in line or 'No such' in line:   # 去除无用输出
                continue
            tag = line.rsplit('/')[-1]
            hiddenFilePath = os.path.join(ldir, '.' + tag)
            normalFilePath = os.path.join(ldir, tag)
            time = line.split('/')[0].replace(' ', '').replace('-', '').replace(':', '')
            with open(hiddenFilePath, 'w') as writer:
                writer.write(time)

            if os.path.isfile(hiddenFilePath):
                os.rename(hiddenFilePath, normalFilePath)
                logger.info("Load tag '%s' to file '%s' success" % (tag, normalFilePath))


if __name__ == '__main__':
    try:

        import logging.config
        from src.parser import conf

        logging.config.fileConfig(conf.get('basic', 'log.conf.path'))

        logger = logging.getLogger('stdout')
        duration = conf.getint('basic', 'sync.duration')
        hdfsPath = conf.get('basic', 'hdfs.tags.path')
        fsPath = conf.get('basic', 'fs.tags.path')
        TagsLoader(hdfsPath, fsPath, duration).run()
    except Exception, e:
        logger.error(str(e))
        exit(-1)