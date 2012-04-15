# -*- coding: utf-8 -*-
"""
test_resumable.py

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

_motoboto = os.environ.get("USE_BOTO", "0") != "1"

if _motoboto:
    import motoboto as boto
    from motoboto.s3.key import Key
    from motoboto.s3.resumable_download_handler import ResumableDownloadHandler
    from lumberyard.http_connection import LumberyardHTTPError \
            as http_exception
else:
    import boto
    from boto.s3.key import Key
    from boto.s3.resumable_download_handler import ResumableDownloadHandler
    from boto.exception import S3ResponseError as http_exception

from tests.test_util import test_dir_path, initialize_logging

class TestResumableDownloadHandler(unittest.TestCase):
    """
    This is a test that motoboto emulates boto S3 functions.

    This test can be run against either an Amazon AWS account or a nimbus.io
    account based on the USE_BOTO environment variable.

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

    def test_uninterrupted_resumable(self):
        """
        test get_contents_to_file without any interruption. 
        """
        log = logging.getLogger("test_uninterrupted_resumable")
        bucket_name = "com-spideroak-test-key-with-files"
        key_name = "test-key"
        test_file_path = os.path.join(
            test_dir_path, "test-orignal"
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
#        self.assertFalse(write_key.exists())

        # upload some data
        with open(test_file_path, "r") as archive_file:
            write_key.set_contents_from_file(archive_file)        
        self.assertTrue(write_key.exists())

        # create a ResumableDownloadHandler
        tracker_file_path = os.path.join(
            test_dir_path, "tracker-file"
        )
        download_handler = ResumableDownloadHandler(
            tracker_file_name=tracker_file_path
        )

        # read back the data
        retrieve_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )
        with open(retrieve_file_path, "w") as retrieve_file:
            write_key.get_contents_to_file(retrieve_file, 
                                           res_download_handler=\
                                            download_handler)      

        self.assertTrue(
            filecmp.cmp(test_file_path, retrieve_file_path, shallow=False)
        )

        # delete the key
        write_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def test_interrupted_resumable(self):
        """
        test get_contents_to_file with a simulated interruption. 
        """
        log = logging.getLogger("test_uninterrupted_resumable")
        bucket_name = "com-spideroak-test-key-with-files"
        key_name = "test-key"
        test_file_path = os.path.join(
            test_dir_path, "test-orignal"
        )
        test_file_size = 1024 ** 2
        interrupted_size = 1024 * 42

        test_data = os.urandom(test_file_size)

        log.debug("writing %s bytes to %s" % (test_file_size, test_file_path,))
        with open(test_file_path, "w") as output_file:
            output_file.write(test_data)

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
#        self.assertFalse(write_key.exists())

        # upload some data
        with open(test_file_path, "r") as archive_file:
            write_key.set_contents_from_file(archive_file)        
        self.assertTrue(write_key.exists())

        # create a ResumableDownloadHandler
        tracker_file_path = os.path.join(
            test_dir_path, "tracker-file"
        )
        download_handler = ResumableDownloadHandler(
            tracker_file_name=tracker_file_path
        )

        retrieve_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )

        # copy some of the data to the retrieve file to simulate an
        # interrupted retrieve
        with open(retrieve_file_path, "w") as output_file:
            output_file.write(test_data[interrupted_size:])

        # spoof the resumable handler into thinking it has a retrieve
        # in progress
        download_handler._save_tracker_info(write_key)

        # resume the retrieve
        with open(retrieve_file_path, "wa") as retrieve_file:
            write_key.get_contents_to_file(retrieve_file, 
                                           res_download_handler=\
                                            download_handler)      

        # read back the retrieved data
        with open(retrieve_file_path, "r") as retrieve_file:
            retrieved_data = retrieve_file.read()

        self.assertEqual(len(retrieved_data), len(test_data))
        self.assertTrue(retrieved_data == test_data)

        # delete the key
        write_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
if __name__ == "__main__":
    initialize_logging()
    unittest.main()

