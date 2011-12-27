# -*- coding: utf-8 -*-
"""
test_multipart.py

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
    from motoboto.s3.key import Key
else:
    import boto
    from boto.s3.key import Key

from tests.test_util import test_dir_path, initialize_logging

class TestMultipart(unittest.TestCase):
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

    def test_simple_multipart(self):
        """
        test a simple multipart upload
        """
        log = logging.getLogger("test_simple_multipart")
        bucket_name = "com-spideroak-test-simple-multipart"
        key_name = "test_key"
        part_count = 2
        path_template = os.path.join(
            test_dir_path, "test_simple_multipart_%02d"
        )
        test_file_paths = [path_template % (n+1, ) for n in range(part_count)]
        retrieve_path = os.path.join(test_dir_path, "retrieve_multipart")
        # 5mb is the minimum size s3 will take 
        test_file_size = 1024 ** 2 * 5
        buffer_size = 1024

        for test_file_path in test_file_paths:
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
        for index, test_file_path in enumerate(test_file_paths):
            with open(test_file_path, "r") as input_file:
                multipart_upload.upload_part_from_file(input_file, index+1)

        # complete the upload
        completed_upload = multipart_upload.complete_upload()
        print >> sys.stderr, dir(completed_upload)

        key = Key(bucket, key_name)
        with open(retrieve_path, "w") as output_file:
            key.get_contents_to_file(output_file)

        # TODO: compare files

        # delete the key
        key.delete()
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
if __name__ == "__main__":
    initialize_logging()
    unittest.main()

