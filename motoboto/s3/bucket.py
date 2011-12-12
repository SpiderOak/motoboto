# -*- coding: utf-8 -*-
"""
bucket.py

simulate a boto Bucket object
"""
import json
import logging

from lumberyard.http_util import compute_default_hostname, \
        compute_collection_hostname, \
        compute_uri
from lumberyard.http_connection import HTTPConnection

from motoboto.s3.bucketlistresultset import BucketListResultSet
from motoboto.s3.key import Key

class Bucket(object):
    """
    wraps a nimbus.io collection to simuate an S3 bucket
    """
    def __init__(self, identity, collection_name):
        self._log = logging.getLogger("Bucket(%s)" % (collection_name, ))
        self._identity = identity
        self._collection_name = collection_name

    @property
    def name(self):
        return self._collection_name

    def get_all_keys(
        self, max_keys=1000, prefix="", marker="", delimiter=""
    ):
        """
        max_keys
            The maximum number of keys to retrieve

        prefix
            The prefix of the keys you want to retrieve

        marker 
            where you are in the result set

        delimiter
        
            Keys that contain the same string between the prefix and the 
            first occurrence of the delimiter will be rolled up into a single 
            result element. 

            These rolled-up keys are not returned elsewhere in the response.

        return a list of all keys in this collection
        """
        method = "GET"

        http_connection = self.create_http_connection()

        kwargs = {
            "max_keys" : max_keys,
        }
        if prefix != "" and prefix is not None: 
            kwargs["prefix"] = prefix
        if marker != "" and marker is not None: 
            kwargs["marker"] = marker
        if delimiter != "" and delimiter is not None: 
            kwargs["delimiter"] = delimiter

        uri = compute_uri("data/", **kwargs)

        response = http_connection.request(method, uri)
        
        data = response.read()
        http_connection.close()
        data_list = json.loads(data)
        return [Key(bucket=self, name=n) for n in data_list]

    def list(self, prefix="", delimiter="", marker=""):
        """
        prefix
            The prefix of the keys you want to retrieve

        marker 
            where you are in the result set

        delimiter
        
            Keys that contain the same string between the prefix and the 
            first occurrence of the delimiter will be rolled up into a single 
            result element. 

            These rolled-up keys are not returned elsewhere in the response.

        return a BucketListResultSet object
        """
        return BucketListResultSet(self, prefix, delimiter, marker)
    
    def get_key(self, name):
        """
        return a key object for the name
        """
        return Key(bucket=self, name=name)
    
    def create_http_connection(self):
        """
        create an HTTP connection with our colection name as the host
        """
        return HTTPConnection(
            compute_collection_hostname(self._collection_name),
            self._identity.user_name,
            self._identity.auth_key,
            self._identity.auth_key_id
        )

    def get_space_used(self):
        """
        get disk space statistics for this collection
        """
        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._identity.user_name,
            self._identity.auth_key,
            self._identity.auth_key_id
        )
        method = "GET"
        uri = compute_uri(
            "/".join([
                "customers", 
                self._identity.user_name, 
                "collections",
                self._collection_name
            ]),
            action="space_usage"
        )

        response = http_connection.request(method, uri)
        data = response.read()
        http_connection.close()
    
        return json.loads(data)

