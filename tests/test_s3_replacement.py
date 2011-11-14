# -*- coding: utf-8 -*-
"""
test_s3_replacement.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and $NAME
"""
import filecmp
import logging
import os
import os.path
import random
import shutil
from cStringIO import StringIO
import sys
import unittest

if os.environ.get("USE_MOTOBOTO", "0") == "1":
    import motoboto as boto
    from motoboto.s3.key import Key
else:
    import boto
    from boto.s3.key import Key

_tmp_path = os.environ.get("TEMP", "/tmp")
_test_dir_path = os.path.join(_tmp_path, "test_s3_replacement")

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    
    logging.root.addHandler(console)

    logging.root.setLevel(logging.DEBUG)

def _random_string(size):
    return "".join([chr(random.randint(0, 255)) for _ in xrange(size)])

class TestS3(unittest.TestCase):
    """test S3 functionality"""

    def setUp(self):
        log = logging.getLogger("setUp")
        self.tearDown()  
        log.debug("creating %s" % (_test_dir_path))
        os.makedirs(_test_dir_path)
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

        if os.path.exists(_test_dir_path):
            shutil.rmtree(_test_dir_path)

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

    def test_key_with_strings(self):
        """
        test simple key 'from_string' and 'as_string' functions
        """
        bucket_name = "com-spideroak-test-key-with-strings"
        key_name = u"test-key"
        test_string = _random_string(1024)

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
            _test_dir_path, "test_key_with_files-orignal"
        )
        test_file_size = 1024 ** 2
        buffer_size = 1024

        log.debug("writing %s bytes to %s" % (
            test_file_size, test_file_path, 
        ))
        bytes_written = 0
        with open(test_file_path, "w") as output_file:
            while bytes_written < test_file_size:
                output_file.write(_random_string(buffer_size))
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
            _test_dir_path, "test_key_with_files-orignal"
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
            _test_dir_path, "test_key_with_files-orignal"
        )
        test_file_size = 1024 ** 2
        buffer_size = 1024

        log.debug("writing %s bytes to %s" % (
            test_file_size, test_file_path, 
        ))
        bytes_written = 0
        with open(test_file_path, "w") as output_file:
            while bytes_written < test_file_size:
                output_file.write(_random_string(buffer_size))
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
            _test_dir_path, "test_key_with_files-orignal"
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
        
    def xxxtest_key_with_meta(self):
        """
        test simple key with metadata added
        """
        bucket_name = "com-spideroak-test-key-with-meta"
        key_name = u"test-key"
        test_string = _random_string(1024)
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
        
    def xxxtest_write_over_key_with_meta(self):
        """
        test that metadata persists when a key is written over
        """
        bucket_name = "com-spideroak-test-key-with-meta"
        key_name = u"test-key"
        test_string = _random_string(1024)
        test_string_1 = _random_string(1024)
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
        self.assertEqual(returned_meta_value, meta_value)

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def xxxtest_simple_multipart(self):
        """
        test a simple multipart upload
        """
        log = logging.getLogger("test_simple_multipart")
        bucket_name = "com-spideroak-test-simple-multipart"
        key_name = "test_key"
        test_file_path = os.path.join(
            _test_dir_path, "test_simple_multipart-orignal"
        )
        part_count = 2
        # 5mb is the minimum size s3 will take 
        test_file_size = 1024 ** 2 * 5 * part_count
        buffer_size = 1024

        log.debug("writing %s bytes to %s" % (
            test_file_size, test_file_path, 
        ))
        bytes_written = 0
        with open(test_file_path, "w") as output_file:
            while bytes_written < test_file_size:
                output_file.write(_random_string(buffer_size))
                bytes_written += buffer_size

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # assert that we have no uploads in progress
        upload_list = bucket.get_all_multipart_uploads()
        self.assertEqual(len(upload_list), 0)

        # start the multipart upload
        multipart_upload = bucket.initiate_multipart_upload(key_name)

        # assert that our upload is in progress
        upload_list = bucket.get_all_multipart_uploads()
        self.assertEqual(len(upload_list), 1)
        self.assertEqual(upload_list[0].id, multipart_upload.id)

        # upload a file in pieces
        current_pos = 0
        part_size = int(test_file_size / part_count)
        for index in range(part_count):
            with open(test_file_path, "r") as input_file:
                input_file.seek(current_pos)
                data = input_file.read(part_size)
            upload_file = StringIO(data)
            multipart_upload.upload_part_from_file(upload_file, index+1)

        # complete the upload
        completed_upload = multipart_upload.complete_upload()
        print >> sys.stderr, dir(completed_upload)

        # delete the key
        key = Key(bucket, key_name)
        key.delete()
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
if __name__ == "__main__":
    _initialize_logging()
    unittest.main()

