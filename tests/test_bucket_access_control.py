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

from tests.test_util import initialize_logging

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

def _archive_key_from_string(collection_name, key_name, data):
    http_connection = \
        UnAuthHTTPConnection(compute_collection_hostname(collection_name))

    kwargs = {"conjoined_identifier"  : None}

    method = "POST"
    uri = compute_uri("data", key_name, **kwargs)

    response = http_connection.request(method, uri, body=data)
    
    response_str = response.read()
    http_connection.close()

    return json.loads(response_str)

def _retrieve_key_to_string(collection_name, key_name):
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

def _head_key(collection_name, key_name):
    http_connection = \
        UnAuthHTTPConnection(compute_collection_hostname(collection_name))

    kwargs = dict()

    method = "HEAD"
    uri = compute_uri("data", key_name, **kwargs)

    response = http_connection.request(method, uri, body=None)
    
    _ = response.read()
    headers = response.getheaders()
    http_connection.close()

    return headers

class TestBucketAccessControl(unittest.TestCase):
    """
    test nimbus.io access_control extensions to bucket
    """

    def setUp(self):
        self.tearDown()  

    def tearDown(self):
        pass

    def test_bucket_without_access_control(self):
        """
        test a bucket that has no access control
        """
        bucket_name = "com-spideroak-test-bucket-no-access-control"
        s3_connection = motoboto.S3Emulator()
        for bucket in s3_connection.get_all_buckets():
            if bucket.name == bucket_name:
               s3_connection.delete_bucket(bucket_name)

        # create the bucket
        bucket = s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # the bucket's authenticated connection should be able to list keys
        _ = bucket.get_all_keys()

        # an unauthenticated connection should be denied list_access
        with self.assertRaises(LumberyardHTTPError) as context_manager:
            _ = _list_keys(bucket_name)
        self.assertEqual(context_manager.exception.status, 401)

        # the bucket's authenticated connection should be able to write
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
            _ = _archive_key_from_string(bucket_name, 
                                       unauth_key_name, 
                                       unauth_test_string)
        self.assertEqual(context_manager.exception.status, 401)

        # the bucket's authenticated connection should be able to read
        read_key = Key(bucket, auth_key_name)
        returned_string = read_key.get_contents_as_string()        
        self.assertEqual(returned_string, auth_test_string)

        # an unauthenticated connection should be denied read_access
        with self.assertRaises(LumberyardHTTPError) as context_manager:
            _ = _retrieve_key_to_string(bucket_name, unauth_key_name) 
        self.assertEqual(context_manager.exception.status, 401)

        # the bucket's authenticated connection should be able to delete
        read_key.delete()        

        # an unauthenticated connection should be denied delete_access
        with self.assertRaises(LumberyardHTTPError) as context_manager:
            _ = _delete_key(bucket_name, unauth_key_name) 
        self.assertEqual(context_manager.exception.status, 401)

        # delete the bucket
        s3_connection.delete_bucket(bucket_name)
        s3_connection.close()

    def test_bucket_with_basic_access_control(self):
        """
        test a bucket that has basic access control
        """
        log = logging.getLogger("setUp")
        bucket_name = "com-spideroak-test-bucket-with-access-control"
        s3_connection = motoboto.S3Emulator()
        for bucket in s3_connection.get_all_buckets():
            if bucket.name == bucket_name:
               s3_connection.delete_bucket(bucket_name)

        basic_access_control = {"version"               : "1.0",
                                "allow_unauth_read"     : True, 
                                "allow_unauth_write"    : True, 
                                "allow_unauth_list"     : True, 
                                "allow_unauth_delete"   : True} 
        basic_access_control_json = json.dumps(basic_access_control)

        # create the bucket
        bucket = s3_connection.create_bucket(
            bucket_name, 
            access_control=basic_access_control_json)

        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # the bucket's authenticated connection should be able to list keys
        _ = bucket.get_all_keys()

        # an unauthenticated connection should also list keys
        _ = _list_keys(bucket_name)

        # the bucket's authenticated connection should be able to write
        auth_key_name = "authenticated_key"
        auth_test_string = "authenticated test string"
        write_key = Key(bucket)
        write_key.name = auth_key_name
        write_key.set_contents_from_string(auth_test_string)        
        self.assertTrue(write_key.exists())

        # an unauthenticated connection should also be able to write
        unauth_key_name = "unauthenticated_key"
        unauth_test_string = "unauth test string"
        archive_result = _archive_key_from_string(bucket_name, 
                                                  unauth_key_name, 
                                                  unauth_test_string)
        self.assertTrue("version_identifier" in archive_result)
        head_result = _head_key(bucket_name, unauth_key_name)
        log.info("head_result = {0}".format(head_result))

        # the bucket's authenticated connection should be able to read
        read_key = Key(bucket, auth_key_name)
        returned_string = read_key.get_contents_as_string()        
        self.assertEqual(returned_string, auth_test_string)

        # an unauthenticated connection should also be able to read
        returned_string = _retrieve_key_to_string(bucket_name, unauth_key_name) 
        self.assertEqual(returned_string, unauth_test_string)

        # the bucket's authenticated connection should be able to delete
        read_key.delete()        

        # an unauthenticated connection should also be able to delete
        delete_result = _delete_key(bucket_name, unauth_key_name) 
        self.assertTrue(delete_result["success"])

        # delete the bucket
        s3_connection.delete_bucket(bucket_name)
        s3_connection.close()


if __name__ == "__main__":
    initialize_logging()
    unittest.main()

