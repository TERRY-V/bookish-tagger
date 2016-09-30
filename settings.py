#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

# SECURITY WARNING: don't run with debug turned on in production!

DEBUG = False

# MySQL Database parameters

DATABASES = {
    'default': {
        'NAME': 'platformDC',
        'USER': 'platformDC_admin',
        'PASSWORD': '123456',
        'HOST': '192.168.1.91',
        'PORT': 3306,
    }
}

# Start time
startTime = '1970-01-01 00:00:00'

# Start time file
startTimeFile = 'start.time.cache'

# Interval(s) for checking database
interval = 60

