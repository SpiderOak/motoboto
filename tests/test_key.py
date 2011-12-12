# -*- coding: utf-8 -*-
"""
test_key.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and nimbus.io
"""
import filecmp
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
    from motoboto.s3.key import Key
else:
    import boto
    from boto.s3.key import Key

from tests.test_util import test_dir_path, initialize_logging

class TestKey(unittest.TestCase):
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

    def test_key_with_strings(self):
        """
        test simple key 'from_string' and 'as_string' functions
        """
        bucket_name = "com-spideroak-test-key-with-strings"
        key_name = u"test-key"
        test_string = os.urandom(1024)

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        # self.assertFalse(write_key.exists())

        # upload some data
        write_key.set_contents_from_string(test_string)        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        returned_string = read_key.get_contents_as_string()      
        self.assertEqual(returned_string, test_string)

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def test_key_with_files(self):
        """
        test simple key 'from_file' and 'to_file' functions
        """
        log = logging.getLogger("test_key_with_files")
        bucket_name = "com-spideroak-test-key-with-files"
        key_name = "A" * 1024
        test_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )
        test_file_size = 1024 ** 2
        buffer_size = 1024

        log.debug("writing %s bytes to %s" % (
            test_file_size, test_file_path, 
        ))
        bytes_written = 0
        with open(test_file_path, "w") as output_file:
            while bytes_written < test_file_size:
                output_file.write(os.urandom(buffer_size))
                bytes_written += buffer_size

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        self.assertFalse(write_key.exists())

        # upload some data
        with open(test_file_path, "r") as archive_file:
            write_key.set_contents_from_file(archive_file)        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        retrieve_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )
        with open(retrieve_file_path, "w") as retrieve_file:
            read_key.get_contents_to_file(retrieve_file)      
        self.assertTrue(
            filecmp.cmp(test_file_path, retrieve_file_path, shallow=False)
        )

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def test_key_with_files_and_callback(self):
        """
        test simple key 'from_file' and 'to_file' functions
        """
        def _archive_callback(bytes_sent, total_bytes):
            print >> sys.stderr, "archived", str(bytes_sent), "out of", \
                    str(total_bytes)

        def _retrieve_callback(bytes_sent, total_bytes):
            print >> sys.stderr, "retrieved", str(bytes_sent), "out of", \
                    str(total_bytes)

        log = logging.getLogger("test_key_with_files")
        bucket_name = "com-spideroak-test-key-with-files-and-callback"
        key_name = "A" * 1024
        test_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )
        test_file_size = 1024 ** 2
        buffer_size = 1024

        log.debug("writing %s bytes to %s" % (
            test_file_size, test_file_path, 
        ))
        bytes_written = 0
        with open(test_file_path, "w") as output_file:
            while bytes_written < test_file_size:
                output_file.write(os.urandom(buffer_size))
                bytes_written += buffer_size

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        self.assertFalse(write_key.exists())

        # upload some data
        with open(test_file_path, "r") as archive_file:
            write_key.set_contents_from_file(
                archive_file, cb=_archive_callback
            )        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        retrieve_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )
        # 2011-08-08 dougfort boto aborts if you don't tell it the size
        read_key.size = test_file_size
        with open(retrieve_file_path, "w") as retrieve_file:
            read_key.get_contents_to_file(
                retrieve_file, cb=_retrieve_callback
            )      
        self.assertTrue(
            filecmp.cmp(test_file_path, retrieve_file_path, shallow=False)
        )

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def test_key_with_meta(self):
        """
        test simple key with metadata added
        """
        bucket_name = "com-spideroak-test-key-with-meta"
        key_name = u"test-key"
        test_string = os.urandom(1024)
        meta_key = u"meta_key"
        meta_value = "pork"

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        # self.assertFalse(write_key.exists())

        # set some metadata
        write_key.set_metadata(meta_key, meta_value)

        # upload some data
        write_key.set_contents_from_string(test_string)        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        returned_string = read_key.get_contents_as_string()      
        self.assertEqual(returned_string, test_string)

        # get the metadata
        returned_meta_value = read_key.get_metadata(meta_key)
        self.assertEqual(returned_meta_value, meta_value)

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def test_write_over_key_with_meta(self):
        """
        test that metadata does not persist when a key is written over
        """
        bucket_name = "com-spideroak-test-key-with-meta"
        key_name = u"test-key"
        test_string = os.urandom(1024)
        test_string_1 = os.urandom(1024)
        meta_key = u"meta_key"
        meta_value = "pork"

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        # self.assertFalse(write_key.exists())

        # set some metadata
        write_key.set_metadata(meta_key, meta_value)

        # upload some data
        write_key.set_contents_from_string(test_string)        
        self.assertTrue(write_key.exists())

        # create another key to write over the first key
        write_key1 = Key(bucket, key_name)

        # upload some data
        write_key1.set_contents_from_string(test_string_1)        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        returned_string = read_key.get_contents_as_string()      
        self.assertEqual(returned_string, test_string_1)

        # get the metadata
        returned_meta_value = read_key.get_metadata(meta_key)
        self.assertEqual(returned_meta_value, None)

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
if __name__ == "__main__":
    initialize_logging()
    unittest.main()

