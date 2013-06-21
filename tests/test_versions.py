# -*- coding: utf-8 -*-
"""
test_versions.py

test some problems that have come up wither versions

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

class TestVersions(unittest.TestCase):
    """
    This is a test of some suspected problems wiht versions.

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

    def test_delete_version_followed_by_archive(self):
        """
        test this sequence
        * create a bucket with versioning 
        * archive a key to the bucket
        * delete the version that was just archived
        * archive a new version
        * verify that the new version is accessible

        """
        log = logging.getLogger("test_delete_version_followed_by_archive")
        key_names = ["test-key1", ]
        iteration_count = 10

        # create the bucket
        bucket = self._s3_connection.create_unique_bucket()
        bucket.configure_versioning(True)
        self.assertTrue(bucket is not None)
        
        # archive a key 
        keys_with_data = _create_some_keys_with_data(bucket, key_names)
        self.assertEqual(len(keys_with_data), 1)
        test_key, test_data = keys_with_data[0]

        for index in range(iteration_count):
            log.info("iteration {0}".format(index+1))

            self.assertTrue(test_key.exists())

            # try to retrieve the key
            read_data = test_key.get_contents_as_string()
            self.assertEqual(read_data, 
                             test_data, 
                             test_key.name)

            # delete the specific version we just archived
            test_key.delete(version_id=test_key.version_id)
            self.assertFalse(test_key.exists())

            # archive another version of the key 
            keys_with_data = _create_some_keys_with_data(bucket, key_names)
            self.assertEqual(len(keys_with_data), 1)
            test_key, test_data = keys_with_data[0]

        _clear_bucket(self._s3_connection, bucket)
                
if __name__ == "__main__":
    initialize_logging()
    unittest.main()
