# -*- coding: utf-8 -*-
"""
test_bucket_get_all_versions.py

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
else:
    import motoboto as boto
    from motoboto.s3.key import Key

from tests.test_util import test_dir_path, initialize_logging

def _create_some_keys_with_data(bucket, key_names):
    keys_with_data = list()
    for key_name in key_names:
        key = Key(bucket)

        # set the name
        key.name = key_name

        # upload some data
        test_string = os.urandom(1024)
        key.set_contents_from_string(test_string)        

        keys_with_data.append((key, test_string, ))

    return keys_with_data 

def _create_some_keys(bucket, key_names):
    keys_with_data = _create_some_keys_with_data(bucket, key_names)
    return [key for (key, _) in keys_with_data]

def _clear_keys(bucket):
    for key in bucket.get_all_versions():
        key.delete()

def _clear_bucket(s3_connection, bucket):
    _clear_keys(bucket)
    s3_connection.delete_bucket(bucket.name)

class TestBucketGetAllVersions(unittest.TestCase):
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

    def test_get_all_versions_empty_bucket(self):
        """
        test get_all_versions() on an empty buckey
        """
        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        bucket.configure_versioning(True)
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)

        # try a simple get_all_versions()
        result = bucket.get_all_versions()
        self.assertEqual(result, [], [str(x) for x in result])

        _clear_bucket(self._s3_connection, bucket)

    def test_multiple_versions_of_one_file(self):
        """
        test that we get multiple versions of a file
        """
        key_names = ["test-key1", "test-key1", "test-key1", ]

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        bucket.configure_versioning(True)
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)
        
        keys_with_data = _create_some_keys_with_data(bucket, key_names)
        keys_with_data_dict = \
            dict([(key.version_id, data, ) for (key, data) in keys_with_data])

        result = bucket.get_all_versions()
        self.assertEqual(len(result), len(key_names))
        for result_key in result:
            read_key = Key(bucket)
            read_key.name = result_key.name
            read_key_data = read_key.get_contents_as_string(
                version_id=result_key.version_id
            )
            self.assertEqual(read_key_data, 
                             keys_with_data_dict[result_key.version_id], 
                             result_key.name)

        _clear_bucket(self._s3_connection, bucket)
        
    def test_get_all_versions_tree(self):
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
        bucket.configure_versioning(True)
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)
        
        keys = _create_some_keys(bucket, key_names)
        
        result = bucket.get_all_versions(prefix="aaa")
        self.assertEqual(len(result), 7)

        result = bucket.get_all_versions(prefix="aaa/b")
        self.assertEqual(len(result), 6)

        result = bucket.get_all_versions(prefix="aaa/b/ccccccccc/")
        self.assertEqual(len(result), 3)

        result = bucket.get_all_versions(prefix="aaa/b/dddd")
        self.assertEqual(len(result), 2)

        result = bucket.get_all_versions(prefix="aaa/e")
        self.assertEqual(len(result), 1)

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
        bucket.configure_versioning(True)
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)
        
        keys = _create_some_keys(bucket, key_names)
        
        result = bucket.get_all_versions(delimiter="/")
        result_names = set()
        for prefix_entry in result:
            result_names.add(prefix_entry.name)
        self.assertEqual(result_names, set(["aaa/", "fff/"]), result_names)

        prefix = "aaa/"
        result = bucket.get_all_versions(prefix=prefix, delimiter="/")
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
        bucket.configure_versioning(True)
        self.assertTrue(bucket is not None)
        _clear_keys(bucket)
        
        keys = _create_some_keys(bucket, key_names)
        
        test_marker = ""
        test_version_id_marker = ""
        result_names = list()
        while True:
            result = bucket.get_all_versions(max_keys=test_max, 
                                             key_marker=test_marker,
                                             version_id_marker=test_version_id_marker)
            if len(result) == 0:
                break
            self.assertTrue(len(result) <= test_max, len(result))
            result_names.extend([key.name for key in result])
            test_marker=result[-1].name
            test_version_id_marker=result[-1].version_id

        self.assertEqual(len(result_names), len(key_names))
        self.assertEqual(set(result_names), set(key_names))

        _clear_bucket(self._s3_connection, bucket)
        
if __name__ == "__main__":
    initialize_logging()
    unittest.main()
