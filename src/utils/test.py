#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from FileUtils import FileUtils
from TimeUtils import TimeUtils
from TimeLimitExecutor import TimeLimitExecutor
from DynamicClassLoader import DynamicClassLoader


class Task(object):

    def __init__(self, n):
        super(Task, self).__init__()
        self.n = n

    def run(self, x):
        time.sleep(20)
        print self.n + 10 + x


if __name__ == '__main__':
    # print FileUtils.countRow('/home/deadend/code/java/basic/')

    # t = Task(2)
    # exe = TimeLimitExecutor(10, t.run, args=(3, ))
    # exe.execute()

    # FileUtils.backup('/tmp/zzc/', '/tmp/zzc_bak', '*2016.txt')

    # print TimeUtils.tsp2time(1472478350916)
    print TimeUtils.timedelta('20160901', 3)
    print TimeUtils.timedelta('201609', 5)

    # FileUtils.addRowIndex('/tmp/files', '|', 10000)

    # print FileUtils.countFilesRow('/tmp/zzc')
    # print FileUtils.countFilesRow('/tmp/zzc/a')

    # print FileUtils.merge('/tmp/zzc', 'abc', 'test*')
    # print FileUtils.split('/tmp/zzc/abc', 1024, 'test_', '.txt', 4)  # 需要在hiveloader根目录执行

    # FileUtils.remove('/tmp/zzc', "b*")
    # FileUtils.rmHiddenFile('/tmp/zzc/')

    # print FileUtils.addExtension('/tmp/zzc/', '.txt', 'test*')

    # print DynamicClassLoader.load('TimeUtils.TimeUtils')
