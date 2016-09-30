#!/usr/bin/env python
# -*- coding: utf-8 -*-

from LazyParser import LazyParser


conf = LazyParser('etc/hiveloader.ini')

WEB_RELOADER_LOADER_BASE = conf.get('coreHiveLoader', 'web.reloader.base.loader')
FS_RELOADER_LOADER_BASE = conf.get('coreHiveLoader', 'fs.reloader.base.loader')
