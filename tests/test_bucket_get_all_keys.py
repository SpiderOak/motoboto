# -*- coding: utf-8 -*-
"""
test_bucket_get_all_keys.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and nimbus.io
"""
from __future__ import print_function
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
else:
    import motoboto as boto
    from motoboto.s3.key import Key

from tests.test_util import test_dir_path, initialize_logging

def _create_some_keys(bucket, key_names):
    keys = list()
    for key_name in key_names:
        key = Key(bucket)

        # set the name
        key.name = key_name

        # upload some data
        test_string = os.urandom(1024)
        key.set_contents_from_string(test_string)        

        keys.append(key)

    return keys 

def _clear_keys(bucket):
    for key in bucket.get_all_keys():
        print("clear {0}".format(key.name))
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
        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)

        # try a simple get_all_keys()
        result = bucket.get_all_keys()
        self.assertEqual(result, [])

        _clear_bucket(self._s3_connection, bucket)

    def test_get_all_keys_max_keys(self):
        """
        test that the max keys parameter restricts the number of keys
        """
        key_names = ["test_key1", "test_key2", "test_key3", ]
        test_max = len(key_names) - 1

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)
        
        _ = _create_some_keys(bucket, key_names)

        result = bucket.get_all_keys(max_keys=test_max)
        self.assertTrue(len(result) <= test_max, len(result))

        _clear_bucket(self._s3_connection, bucket)
        
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
        _clear_keys(bucket)
        
        _ = _create_some_keys(bucket, key_names)
        
        result = bucket.get_all_keys(prefix="aaa")
        self.assertEqual(len(result), 7)

        result = bucket.get_all_keys(prefix="aaa/b")
        self.assertEqual(len(result), 6)

        result = bucket.get_all_keys(prefix="aaa/b/ccccccccc/")
        self.assertEqual(len(result), 3)

        result = bucket.get_all_keys(prefix="aaa/b/dddd")
        self.assertEqual(len(result), 2)

        result = bucket.get_all_keys(prefix="aaa/e")
        self.assertEqual(len(result), 1)

        result = bucket.get_all_keys()
        self.assertEqual(len(result), 8, result)

        _clear_bucket(self._s3_connection, bucket)
        
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
        _clear_keys(bucket)
        
        _ = _create_some_keys(bucket, key_names)
        
        result = bucket.get_all_keys(delimiter="/")
        result_names = set()
        for prefix_entry in result:
            result_names.add(prefix_entry.name)
        self.assertEqual(result_names, set(["aaa/", "fff/"]), result_names)

        prefix = "aaa/"
        result = bucket.get_all_keys(prefix=prefix, delimiter="/")
        result_names = set()
        for prefix_entry in result:
            result_names.add(prefix_entry.name)
        self.assertEqual(result_names, set(["aaa/b/", "aaa/e/"]))

        _clear_bucket(self._s3_connection, bucket)
        
    def test_marker(self):
        """
        test using a marker
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
        test_max = 3

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)
        
        _ = _create_some_keys(bucket, key_names)
        
        test_marker = ""
        result_names = list()
        while True:
            result = bucket.get_all_keys(max_keys=test_max, marker=test_marker)
            if len(result) == 0:
                break
            self.assertTrue(len(result) <= test_max)
            result_names.extend([key.name for key in result])
            test_marker = result[-1].name

        self.assertEqual(len(result_names), len(key_names))
        self.assertEqual(set(result_names), set(key_names))

        _clear_bucket(self._s3_connection, bucket)
        
if __name__ == "__main__":
    initialize_logging()
    unittest.main()
