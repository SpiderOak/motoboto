# -*- coding: utf-8 -*-
"""
test_bucket_list_result_set.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and nimbus.io
"""
import logging
import os
import os.path
import shutil
try:
    import unittest2 as unittest
except ImportError:
    import unittest

if os.environ.get("USE_BOTO", "0") == "1":
    import boto
    from boto.s3.key import Key
    from boto.s3.bucketlistresultset import BucketListResultSet
else:
    import motoboto as boto
    from motoboto.s3.key import Key
    from motoboto.s3.bucketlistresultset import BucketListResultSet

from tests.test_util import test_dir_path, initialize_logging

def _clear_keys(bucket):
    for key in bucket.get_all_keys():
        key.delete()

def _clear_bucket(s3_connection, bucket):
    _clear_keys(bucket)
    s3_connection.delete_bucket(bucket.name)

class TestBucketGetAllKeys(unittest.TestCase):
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

    def test_get_all_keys_empty_bucket(self):
        """
        test get_all_keys() on an empty buckey
        """
        log = logging.getLogger("empty")

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(bucket is not None)
        for key in bucket.list():
            key.delete()

        # try a simple get_all_keys()
        result_set = BucketListResultSet(bucket)
        self.assertEqual(list(result_set), [])

        # delete the bucket
        self._s3_connection.delete_bucket(bucket.name)

    def test_get_all_keys(self):
        """
        test that we can retriev e all keys
        """
        key_names = ["test-key1", "test_key2", "test_key3", ]

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(bucket is not None)
        for key in bucket.list():
            key.delete()
        
        # create some keys
        keys = list()
        for key_name in key_names:
            key = Key(bucket)

            # set the name
            key.name = key_name

            # upload some data
            test_string = os.urandom(1024)
            key.set_contents_from_string(test_string)        
            self.assertTrue(key.exists())

            keys.append(key)
        
        result_set = bucket.get_all_keys()
        self.assertEqual(len(list(result_set)), 3)

        # delete the keys
        for key in bucket.list():
            key.delete()
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket.name)
        
    def test_get_all_keys_tree(self):
        """
        test storing and retrieving a directory tree
        """
        # 2011-12-04 -- s3 clips leading slash
        key_names = [
            "aaa/b/cccc/1", 
            "aaa/b/ccccccccc/1", 
            "aaa/b/ccccccccc/2", 
            "aaa/b/ccccccccc/3", 
            "aaa/b/dddd/1", 
            "aaa/b/dddd/2", 
            "aaa/e/ccccccccc/1", 
            "fff/e/ccccccccc/1", 
        ]

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(bucket is not None)
        for key in bucket.list():
            key.delete()
        
        # create some keys
        keys = list()
        for key_name in key_names:
            key = Key(bucket)

            # set the name
            key.name = key_name

            # upload some data
            test_string = os.urandom(1024)
            key.set_contents_from_string(test_string)        
            self.assertTrue(key.exists())

            keys.append(key)
        
        result_set = BucketListResultSet(bucket, prefix="aaa")
        self.assertEqual(len(list(result_set)), 7)

        result_set = BucketListResultSet(bucket, prefix="aaa/b")
        self.assertEqual(len(list(result_set)), 6)

        result_set = BucketListResultSet(bucket, prefix="aaa/b/ccccccccc/")
        self.assertEqual(len(list(result_set)), 3)

        result_set = BucketListResultSet(bucket, prefix="aaa/b/dddd")
        self.assertEqual(len(list(result_set)), 2)

        result_set = BucketListResultSet(bucket, prefix="aaa/e")
        self.assertEqual(len(list(result_set)), 1)

        # delete the keys
        for key in bucket.list():
            key.delete()
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket.name)
        
    def test_delimiter(self):
        """
        test using a delimiter
        """
        # 2011-12-04 -- s3 clips leading slash
        key_names = [
            "aaa/b/cccc/1", 
            "aaa/b/ccccccccc/1", 
            "aaa/b/ccccccccc/2", 
            "aaa/b/ccccccccc/3", 
            "aaa/b/dddd/1", 
            "aaa/b/dddd/2", 
            "aaa/e/ccccccccc/1", 
            "fff/e/ccccccccc/1", 
        ]

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(bucket is not None)
        for key in bucket.list():
            key.delete()
        
        # create some keys
        keys = list()
        for key_name in key_names:
            key = Key(bucket)

            # set the name
            key.name = key_name

            # upload some data
            test_string = os.urandom(1024)
            key.set_contents_from_string(test_string)        
            self.assertTrue(key.exists())

            keys.append(key)
        
        result_set = bucket.list(delimiter="/")
        result_names = set()
        for prefix_entry in result_set:
            result_names.add(prefix_entry.name)
        self.assertEqual(result_names, set(["aaa/", "fff/"]))

        # delete the keys
        for key in bucket.list():
            key.delete()
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket.name)
        
if __name__ == "__main__":
    initialize_logging()
    unittest.main()

