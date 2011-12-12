# -*- coding: utf-8 -*-
"""
test_bucket.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and nimbus.io
"""
import logging
import os
import os.path
import shutil
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

if os.environ.get("USE_MOTOBOTO", "0") == "1":
    import motoboto as boto
else:
    import boto

from tests.test_util import test_dir_path, initialize_logging

class TestBucket(unittest.TestCase):
    """
    This is a test that motoboto emulates boto S3 functions.

    This test can be run against either an Amazon AWS account or a nimbus.io
    account based on the USE_MOTOBOTO environment variable.

    Note that you will have to supply unique bucket names.
    """

    def setUp(self):
        log = logging.getLogger("setUp")
        self.tearDown()  
        log.debug("creating %s" % (test_dir_path))
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
        bucket_name = "com-spideroak-test-bucket"

        # list all buckets, ours shouldn't be there
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertFalse(bucket_in_list)

        # create the bucket
        new_bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(new_bucket is not None)
        self.assertEqual(new_bucket.name, bucket_name)
        
        # list all buckets, ours should be there
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertTrue(bucket_in_list)

        # create a duplicate bucket
        # s3 accepts this
        x = self._s3_connection.create_bucket(bucket_name)
        self.assertEqual(x.name, new_bucket.name)

        # list all buckets, ours should be there
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertTrue(bucket_in_list)

        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
        # list all buckets, ours should be gone
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertFalse(bucket_in_list)

if __name__ == "__main__":
    initialize_logging()
    unittest.main()

