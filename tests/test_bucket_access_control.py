# -*- coding: utf-8 -*-
"""
test_bucket_access_control.py

test nimbus.io access_control extensions to bucket
"""
import httplib
import json
import logging
import os
import os.path
import shutil
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from lumberyard.http_connection import UnAuthHTTPConnection, \
        LumberyardHTTPError
from lumberyard.http_util import compute_collection_hostname, \
        compute_uri

_motoboto = os.environ.get("USE_BOTO", "0") != "1"
assert _motoboto
import motoboto
from motoboto.s3.key import Key

from tests.test_util import test_dir_path, initialize_logging

_read_buffer_size = 64 * 1024

def _list_keys(collection_name):
    http_connection = \
        UnAuthHTTPConnection(compute_collection_hostname(collection_name))

    method = "GET"

    kwargs = {"max_keys" : 1000, }

    uri = compute_uri("data/", **kwargs)

    response = http_connection.request(method, uri)
    
    data = response.read()
    http_connection.close()
    return json.loads(data)

def _write_key_from_string(collection_name, key_name, data):
    http_connection = \
        UnAuthHTTPConnection(compute_collection_hostname(collection_name))

    kwargs = {"conjoined_identifier"  : None}

    method = "POST"
    uri = compute_uri("data", key_name, **kwargs)

    response = http_connection.request(method, uri, body=data)
    
    response_str = response.read()
    http_connection.close()

    return json.loads(response_str)

def _read_key_to_string(collection_name, key_name):
    http_connection = \
        UnAuthHTTPConnection(compute_collection_hostname(collection_name))

    kwargs = {"version_identifier"    : None, }
    headers = {}
    expected_status = httplib.OK

    method = "GET"
    uri = compute_uri("data", key_name, **kwargs)

    response = http_connection.request(method, 
                                       uri, 
                                       body=None, 
                                       headers=headers,
                                       expected_status=expected_status)
        
    body_list = list()
    while True:
        data = response.read(_read_buffer_size)
        if len(data) == 0:
            break
        body_list.append(data)

    http_connection.close()

    return "".join(body_list)

def _delete_key(collection_name, key_name):
    http_connection = \
        UnAuthHTTPConnection(compute_collection_hostname(collection_name))

    kwargs = dict()

    method = "DELETE"
    uri = compute_uri("data", key_name, **kwargs)

    response = http_connection.request(method, uri, body=None)
    
    data = response.read()
    http_connection.close()
    return json.loads(data)

class TestBucketAccessControl(unittest.TestCase):
    """
    test nimbus.io access_control extensions to bucket
    """

    def setUp(self):
        log = logging.getLogger("setUp")
        self.tearDown()  
        log.debug("creating %s" % (test_dir_path))
        os.makedirs(test_dir_path)

    def tearDown(self):
        log = logging.getLogger("tearDown")
        if os.path.exists(test_dir_path):
            shutil.rmtree(test_dir_path)

    def test_bucket_without_access_control(self):
        """
        test a bucket that has no access control
        """
        bucket_name = "com-spideroak-test-bucket-no-access-control"
        s3_connection = motoboto.S3Emulator()

        # create the bucket
        new_bucket = s3_connection.create_bucket(bucket_name)
        self.assertTrue(new_bucket is not None)
        self.assertEqual(new_bucket.name, bucket_name)

        # the bucket's authenticated connection should be able to list keys
        _ = new_bucket.get_all_keys()

        # an unauthenticated connection should be denied list_access
        with self.assertRaises(LumberyardHTTPError) as context_manager:
            _ = _list_keys(bucket_name)
        assert context_manager.exception.status == 401 #Unauthorized

        # the bucket's authenticated connection should be able write
        auth_key_name = "authenticated_key"
        auth_test_string = "authenticated test string"
        write_key = Key(bucket)
        write_key.name = auth_key_name
        write_key.set_contents_from_string(auth_test_string)        
        self.assertTrue(write_key.exists())

        # an unauthenticated connection should be denied write_access
        unauth_key_name = "unauthenticated_key"
        unauth_test_string = "unauth test string"
        with self.assertRaises(LumberyardHTTPError) as context_manager:
            _ = _write_key_from_string(bucket_name, 
                                       unath_key_name, 
                                       unauth_test_string)
        assert context_manager.exception.status == 401 #Unauthorized

        # the bucket's authenticated connection should be able to read
        read_key = Key(bucket, auth_key_name)
        returned_string = read_key.get_contents_as_string()        
        self.assertEqual(returned_string, auth_test_string)

        # an unauthenticated connection should be denied read_access
        with self.assertRaises(LumberyardHTTPError) as context_manager:
            _ = _read_key_to_string(bucket_name, unath_key_name) 
        assert context_manager.exception.status == 401 #Unauthorized

        # the bucket's authenticated connection should be able to delete
        read_key.delete()        

        # an unauthenticated connection should be denied delete_access
        with self.assertRaises(LumberyardHTTPError) as context_manager:
            _ = _delete_key(bucket_name, unath_key_name) 
        assert context_manager.exception.status == 401 #Unauthorized

        # delete the bucket
        s3_connection.delete_bucket(bucket_name)
        s3_connection.close()

if __name__ == "__main__":
    initialize_logging()
    unittest.main()

