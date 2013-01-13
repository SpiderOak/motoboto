# -*- coding: utf-8 -*-
"""
test_upload_interval.py

test various upload speeds 
"""
try:
    from httplib import REQUEST_TIMEOUT
except ImportError:
    from http.client import REQUEST_TIMEOUT
from itertools import cycle, islice
import logging
import os
from string import printable
import time

try:
    import unittest2 as unittest
except ImportError:
    import unittest

assert os.environ.get("USE_BOTO", "0") == "0"

import motoboto
from motoboto.s3.key import Key
from lumberyard.http_connection import LumberyardHTTPError

from tests.test_util import initialize_logging

class MockInputFile(object):
    """
    a file-like object that takes a specified amount of time to read
    """
    def __init__(self, sequence_size, sequence_count, total_seconds):
        self._log = logging.getLogger("MockInputFile")
        self._sequence_size = sequence_size
        self._sequence_count = sequence_count
        self._total_seconds = total_seconds
        self._seconds_per_sequence = \
            self._total_seconds / self._sequence_count + 1.0
        self._start_time = None
        self._total_size = self._sequence_size * self._sequence_count
        self._bytes_read = 0

        # don't use the resources needed for random data
        self._source = cycle(printable)

    def read(self, size=None):
        if self._start_time is None: 
            self._start_time = int(time.time())
        elapsed_time = int(time.time()) - self._start_time

        bytes_remaining = self._total_size - self._bytes_read
        if bytes_remaining == 0:
            if elapsed_time < self._total_seconds:
                sleep_time = self._total_seconds - elapsed_time
                self._log.info("sleeping {0}".format(sleep_time))
                time.sleep(sleep_time)
            return ""

        sequences_read = self._bytes_read / self._sequence_size
        sequence_time = sequences_read * self._seconds_per_sequence
        if elapsed_time < sequence_time:
            sleep_time = sequence_time - elapsed_time
            self._log.info("sleeping {0}".format(sleep_time))
            time.sleep(sleep_time)

        bytes_remaining = bytes_remaining % self._sequence_size
        if bytes_remaining == 0:
            bytes_remaining = self._sequence_size

        if size is None or size >= bytes_remaining:
            self._bytes_read += bytes_remaining
            data = "".join(islice(self._source, bytes_remaining))
            return data

        self._bytes_read += size

        data = "".join(islice(self._source, size))
        return data

    def __len__(self):
        return self._total_size

class TestUploadInterval(unittest.TestCase):
    """
    This is a test of working wiht the nimbus.io server for efficient upload.
    """

    def setUp(self):
        log = logging.getLogger("setUp")
        log.debug("start")
        self.tearDown()  
        self._s3_connection = motoboto.connect_s3()
        log.debug("finish")

    def tearDown(self):
        log = logging.getLogger("tearDown")
        log.debug("start")
        if hasattr(self, "_s3_connection") \
        and self._s3_connection is not None:
            log.debug("closing s3 connection")
            self._s3_connection.close()
            self._s3_connection = None
        log.debug("finish")

    def test_fast_upload(self):
        """
        the fastest upload we can manage
        """
        key_name = "test-key"
        sequence_size = 3 * 10 * 1024 * 1024  
        sequence_count = 1 
        total_seconds = 0.0

        write_key = Key(self._s3_connection.default_bucket)
        write_key.name = key_name

        test_file = MockInputFile(sequence_size, sequence_count, total_seconds)

        write_key.set_contents_from_file(test_file)        
        self.assertTrue(write_key.exists())

    def test_well_behaved_upload(self):
        """
        an upload that sends sequences of 10mb 
        """
        key_name = "test-key"
        sequence_size = 10 * 1024 * 1024  
        sequence_count = 3 
        total_seconds = 3 * 301

        write_key = Key(self._s3_connection.default_bucket)
        write_key.name = key_name

        test_file = MockInputFile(sequence_size, sequence_count, total_seconds)

        write_key.set_contents_from_file(test_file)        
        self.assertTrue(write_key.exists())

    def test_slow_upload(self):
        """
        an upload that sends less than 10mb in 5 min
        """
        key_name = "test-key"
        sequence_size = 1024  
        sequence_count = 3 * 10 
        total_seconds = 3 * 10 * 60

        write_key = Key(self._s3_connection.default_bucket)
        write_key.name = key_name

        test_file = MockInputFile(sequence_size, sequence_count, total_seconds)

        with self.assertRaises(LumberyardHTTPError) as context_manager:
            write_key.set_contents_from_file(test_file)

        self.assertEqual(context_manager.exception.status, REQUEST_TIMEOUT)

if __name__ == "__main__":
    initialize_logging()
    unittest.main()

