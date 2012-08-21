# -*- coding: utf-8 -*-
"""
util.py

utility functions used by motoboto
"""
from datetime import datetime

_http_timestamp_format = "%a, %d %b %Y %H:%M:%S GMT"

def http_timestamp_str(timestamp):
    return timestamp.strftime(_http_timestamp_format)

def parse_http_timestamp(timestamp_str):
    return datetime.strptime(timestamp_str, _http_timestamp_format)

