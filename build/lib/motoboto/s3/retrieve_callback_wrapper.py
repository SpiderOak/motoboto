# -*- coding: utf-8 -*-
"""
retrieve_callback_wrapper.py

class RetrieveCallbackWrapper
"""

class NullCallbackWrapper(object):
    """
    A callback wrapper that does nothing. for when we don't have a callback,
    """
    def __init__(self):
        pass

    def start(self):
        pass

    def bytes_written(self, _bytes_written):
        pass

    def finish(self):
        pass

class RetrieveCallbackWrapper(object):
    """
    """
    def __init__(self, size, cb, cb_count):
        self._size = size
        self._callback = cb
        self._callback_count = cb_count
        self._total_bytes_written = 0

    def start(self):
        self._callback(self._total_bytes_written, self._size)

    def bytes_written(self, bytes_written):
        self._total_bytes_written += bytes_written
        # TODO: use callback_count
        self._callback(self._total_bytes_written, self._size)

    def finish(self):
        self._callback(self._total_bytes_written, self._size)

        
