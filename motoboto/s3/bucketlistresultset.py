# -*- coding: utf-8 -*-
"""
BucketListResultSet
"""
import logging

class BucketListResultSet(object):
    """
    The result listmatch
    """
    def __init__(self, bucket, prefix="", delimiter="", marker=""):
        self._log = logging.getLogger("BucketListResultSet")
        self._bucket = bucket
        self._prefix = prefix
        self._delimiter = delimiter
        self._marker = marker

    def __iter__(self):
        more_data = True
        while more_data:
            result = self._bucket.get_all_keys(
                prefix=self._prefix, 
                delimiter=self._delimiter, 
                marker=self._marker
            )

            if len(result) == 0 or self._delimiter != "":
                more_data = False
            else:
                more_data = result.truncated
                self._marker = result[-1].name

            for key in result:
                yield key

