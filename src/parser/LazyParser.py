#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser


class LazyParser:

    def __init__(self, filepath):
        self.cf = None
        self.filepath = filepath

    def __get(self, section, option, default, func=str):
        if self.cf == None:
            cf = ConfigParser.ConfigParser()
            cf.read(self.filepath)
        try:
            return func(cf.get(section, option))
        except Exception, e:
            return default

    def __str2bool__(self, string):
        if string not in ['true', 'false']:
            raise Exception('Param %s cannot be converted to bool' % string)
        return True if string == 'true' else False

    def get(self, section, option, default=None):
        return self.__get(section, option, default)

    def getint(self, section, option, default=None):
        return self.__get(section, option, default, int)

    def getbool(self, section, option, default=None):
        return self.__get(section, option, default, self.__str2bool__)
