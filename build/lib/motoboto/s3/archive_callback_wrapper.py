# -*- coding: utf-8 -*-
"""
archive_callback_wrapper.py

class ArchiveCallbackWrapper

wrap a boto style callback for progress reporting
"""


class ArchiveCallbackWrapper(object):
    """
    wrap a boto style callback for progress reporting
    """
    def __init__(self, reader, cb, cb_count):
        self._reader = reader
        self._reader.set_callback(self._internal_callback)
        self._external_callback = cb
        self._callback_count = cb_count

        self._total_bytes_read = 0

    def _internal_callback(self, bytes_read):
        self._total_bytes_read += bytes_read
        # TODO: use callback_count to control the callbacks
        self._external_callback(self._total_bytes_read, 42)

