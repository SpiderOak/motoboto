# -*- coding: utf-8 -*-
"""
test_modified.py

test RFC 2616 section 14 headers
 * 14.25 If-Modified-Since
 * 14.28 If-Unmodified-Since.

note this is a motoboto extension, it does not exist in boto
"""
import logging
import os
import os.path
import shutil
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest

_motoboto = os.environ.get("USE_BOTO", "0") != "1"

if _motoboto:
    import motoboto as boto
    from motoboto.s3.key import Key, KeyUnmodified, KeyModified

from tests.test_util import test_dir_path, initialize_logging

class TestModified(unittest.TestCase):
    """
    test restricting access by date modified
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

    def _set_up_single_archive(self, bucket_name, key_name):
        test_file_path = os.path.join(
            test_dir_path, "test-orignal"
        )
        test_file_size = 1024 ** 2

        test_data = os.urandom(test_file_size)

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

        return test_data, write_key

    def _tear_down_archive(self, key):
        # delete the key
        key.delete()
        self.assertFalse(key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(key._bucket.name)

    @unittest.skipIf(not _motoboto, "motoboto only")
    def test_modification_restriction(self):
        """
        test restrictions on date modified
        """
        bucket_name = "com-spideroak-modified"
        key_name = "test-key.jpg"

        # this is a time before the file was last modified
        before_time = time.time()

        time.sleep(1.0)

        test_data, key = self._set_up_single_archive(bucket_name, key_name)

        time.sleep(1.0)

        # this is a time after the file was last modified
        after_time = time.time()

        # test the various forms of HEAD
        
        result = key.exists(modified_since=before_time)
        self.assertTrue(result, "modified_since=before_time")
        
        result = key.exists(modified_since=after_time)
        self.assertFalse(result, "modified_since=after_time")

        result = key.exists(unmodified_since=after_time)
        self.assertTrue(result, "unmodified_since=after_time")
        
        result = key.exists(unmodified_since=before_time)
        self.assertFalse(result, "unmodified_since=before_time")
        
        # test the various forms of retrieve

        result = key.get_contents_as_string(modified_since=before_time)
        self.assertEqual(result, test_data)

        self.assertRaises(KeyUnmodified, 
                          key.get_contents_as_string,
                          modified_since=after_time)

        result = key.get_contents_as_string(unmodified_since=after_time)
        self.assertEqual(result, test_data)

        self.assertRaises(KeyModified, 
                          key.get_contents_as_string,
                          modified_since=before_time)

        self._tear_down_archive(key)


if __name__ == "__main__":
    initialize_logging()
    unittest.main()


