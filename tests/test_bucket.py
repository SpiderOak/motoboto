# -*- coding: utf-8 -*-
"""
test_bucket.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and nimbus.io
"""
from __future__ import print_function

import logging
import os
import os.path
import shutil
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

if os.environ.get("USE_BOTO", "0") == "1":
    import boto
else:
    import motoboto as boto

from tests.test_util import test_dir_path, initialize_logging

class TestBucket(unittest.TestCase):
    """
    This is a test that motoboto emulates boto S3 functions.

    This test can be run against either an Amazon AWS account or a nimbus.io
    account based on the USE_BOTO environment variable.

    Note that you will have to supply unique bucket names.
    """

    def setUp(self):
        log = logging.getLogger("setUp")
        self.tearDown()  
        log.debug("creating {0}".format(test_dir_path))
        os.makedirs(test_dir_path)
        log.debug("opening s3 connection")
        self._s3_connection = boto.connect_s3()

    def tearDown(self):
        log = logging.getLogger("tearDown")
        if hasattr(self, "_s3_connection") \
        and self._s3_connection is not None:
            log.debug("closing s3 connection")
            try:
                self._s3_connection.close()
            except AttributeError:
                # 2011-08-04 dougfort -- boto 2.0 chokes if there are no
                # open http connections
                pass
            self._s3_connection = None

        if os.path.exists(test_dir_path):
            shutil.rmtree(test_dir_path)

    def test_bucket(self):
        """
        test basic bucket handling
        """
        # create the bucket
        new_bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(new_bucket is not None)
        
        # list all buckets, ours should be there
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print("bucket list")
        for bucket in bucket_list:
            print("    {0}".format(bucket.name))
            if bucket.name == new_bucket.name:
                bucket_in_list = True
        self.assertTrue(bucket_in_list)

        # delete the bucket
        self._s3_connection.delete_bucket(new_bucket.name)
        
        # list all buckets, ours should be gone
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print("bucket list")
        for bucket in bucket_list:
            print("    {0}".format(bucket.name))
            if bucket.name == new_bucket.name:
                bucket_in_list = True
        self.assertFalse(bucket_in_list)

if __name__ == "__main__":
    initialize_logging()
    unittest.main()

