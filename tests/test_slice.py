# -*- coding: utf-8 -*-
"""
test_slice.py

test downloading a slice from a key

note slice is a motoboto extension, it does not exist in boto
"""
import logging
import os
import os.path
import shutil
try:
    import unittest2 as unittest
except ImportError:
    import unittest

_motoboto = os.environ.get("USE_BOTO", "0") != "1"

if _motoboto:
    import motoboto as boto
    from motoboto.s3.key import Key

from tests.test_util import test_dir_path, initialize_logging

_multipart_part_count = 5
_multipart_part_size = 1024 ** 2 * 5

class TestSlice(unittest.TestCase):
    """
    test downloading a slice from a key
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

    def _set_up_single_archive(self, bucket_name):
        key_name = "test-key"
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

    def _set_up_multipart_archive(self, bucket_name):
        key_name = "test_key"
        path_template = os.path.join(
            test_dir_path, "test_simple_multipart_%02d"
        )
        test_file_paths = [
            path_template % (n+1, ) for n in range(_multipart_part_count)
        ]
        # 5mb is the minimum size s3 will take 
        test_blobs = [
            os.urandom(_multipart_part_size) \
            for _ in range(_multipart_part_count)
        ]

        for test_file_path, test_blob in zip(test_file_paths, test_blobs):
            with open(test_file_path, "w") as output_file:
                output_file.write(test_blob)

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
        multipart_upload.complete_upload()

        return "".join(test_blobs), bucket.get_key(key_name)

    @unittest.skipIf(not _motoboto, "motoboto only")
    def test_entire_single_part(self):
        """
        test get_contents_to_file for the whole archive
        """
        bucket_name = "com-spideroak-test-entire-single-part"
        test_data, key = self._set_up_single_archive(bucket_name)

        # read back the data
        retrieve_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )
        with open(retrieve_file_path, "w") as retrieve_file:
            key.get_contents_to_file(retrieve_file) 

        # read back the retrieved data
        with open(retrieve_file_path, "r") as retrieve_file:
            retrieved_data = retrieve_file.read()

        self.assertEqual(len(retrieved_data), len(test_data))
        self.assertTrue(retrieved_data == test_data)

        self._tear_down_archive(key)

    @unittest.skipIf(not _motoboto, "motoboto only")
    def test_slice_single_part(self):
        """
        test get_contents_to_file for various slices
        """
        bucket_name = "com-spideroak-test-slice-single-part"
        test_data, key = self._set_up_single_archive(bucket_name)
        test_params = [(0, 1024), (1024, 2048), (len(test_data)-2048, 2048)]

        for slice_offset, slice_size in test_params:
            # read back the data
            retrieve_file_path = os.path.join(
                test_dir_path, "test_key_with_files-orignal"
            )
            with open(retrieve_file_path, "w") as retrieve_file:
                key.get_contents_to_file(retrieve_file, 
                                         slice_offset=slice_offset, 
                                         slice_size=slice_size) 

            # read back the retrieved data
            with open(retrieve_file_path, "r") as retrieve_file:
                retrieved_data = retrieve_file.read()

            self.assertEqual(len(retrieved_data), 
                             len(test_data[slice_offset:slice_offset+slice_size]),
                             (len(retrieved_data), slice_offset, slice_size, ))
            self.assertTrue(retrieved_data == \
                            test_data[slice_offset:slice_offset+slice_size],
                            (slice_offset, slice_size, ))

        self._tear_down_archive(key)

    @unittest.skipIf(not _motoboto, "motoboto only")
    def test_entire_multipart(self):
        """
        test get_contents_to_file for the whole archive
        """
        bucket_name = "com-spideroak-test-slice-entire-multipart"
        test_data, key = self._set_up_multipart_archive(bucket_name)

        # read back the data
        retrieve_file_path = os.path.join(
            test_dir_path, "test_key_with_files-orignal"
        )
        with open(retrieve_file_path, "w") as retrieve_file:
            key.get_contents_to_file(retrieve_file) 

        # read back the retrieved data
        with open(retrieve_file_path, "r") as retrieve_file:
            retrieved_data = retrieve_file.read()

        self.assertEqual(len(retrieved_data), len(test_data))
        self.assertTrue(retrieved_data == test_data)

        self._tear_down_archive(key)

    @unittest.skipIf(not _motoboto, "motoboto only")
    def test_slice_multipart(self):
        """
        test get_contents_to_file for various slices of multipart
        """
        bucket_name = "com-spideroak-test-slice-multipart"
        test_data, key = self._set_up_multipart_archive(bucket_name)
        # the points where multiparts join
        seams = range(0, 
                      _multipart_part_size*_multipart_part_count, 
                      _multipart_part_size) 
        test_params = list()
        for seam in seams:
            # a piece off the front
            test_params.append((seam, seam+1024))
            # the whole part
            test_params.append((seam, _multipart_part_size))
            # a piece off the back
            test_params.append((seam+_multipart_part_size-2048, 2048))

        # some overlapping slices
        test_params.append((seams[1]-1024, 2048))
        test_params.append((seams[2], 2*_multipart_part_size+1024))

        for slice_offset, slice_size in test_params:
            # read back the data
            retrieve_file_path = os.path.join(
                test_dir_path, "test_key_with_files-orignal"
            )
            with open(retrieve_file_path, "w") as retrieve_file:
                key.get_contents_to_file(retrieve_file, 
                                         slice_offset=slice_offset, 
                                         slice_size=slice_size) 

            # read back the retrieved data
            with open(retrieve_file_path, "r") as retrieve_file:
                retrieved_data = retrieve_file.read()

            self.assertEqual(len(retrieved_data), 
                             len(test_data[slice_offset:slice_offset+slice_size]),
                             (len(retrieved_data), slice_offset, slice_size, ))
            self.assertTrue(retrieved_data == \
                            test_data[slice_offset:slice_offset+slice_size],
                            (slice_offset, slice_size, ))

        self._tear_down_archive(key)

if __name__ == "__main__":
    initialize_logging()
    unittest.main()

