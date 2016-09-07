#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
运行时间受限的执行器，负责在任务执行时间超限时杀死任务(多进程实现)

@author: zhangzichao
@date: 2016-09-07
'''


import multiprocessing


class TimeLimitExecutor(object):

    """
    初始化
    @param timeLimit: 任务执行时限
    @param target: 待执行任务
    @param name: 任务名称
    @param args: 元组参数
    @param kwargs: 字典参数

    @return 任务退出码: 0正常退出, 1程序异常, -n进程被信号n结束, None执行超时被结束
    """
    def __init__(self, timeLimit, target, name=None, args=(), kwargs={}):
        self.__timeLimit = timeLimit
        self.__target = target
        self.__name = name
        self.__args = args
        self.__kwargs = kwargs


    """
    执行任务
    """
    def execute(self):
        p = multiprocessing.Process(target=self.__target,
                                    name=self.__name,
                                    args=self.__args,
                                    kwargs=self.__kwargs)
        p.start()
        p.join(self.__timeLimit)
        if p.is_alive():
            p.terminate()
        return p.exitcode
