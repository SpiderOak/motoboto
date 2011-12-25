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
from motoboto.s3.multipart import MultiPartUpload

class Prefix(object):
    """
    represent a prefix derived from use of the delimiter argument to 
    get_all_keys
    """
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name

class TruncatableList(list):
    """
    A list of Keys that has the additional attribute 'truncated', indicating
    that more Keys can be listed.
    """
    def __init__(self, *args, **kwargs):
        super(TruncatableList, self).__init__(*args, **kwargs)
        self.truncated = False

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
            The prfix of the keys you want to retrieve

        marker 
            where you are in the result set

        delimiter
        
            Keys that contain the same string between the prefix and the 
            first occurrence of the delimiter will be rolled up into a single 
            result element. 

            These rolled-up keys are not returned elsewhere in the response.

        return 
            TruncatableList : a list of Keys() with an additional attribute
            `truncated`. If truncated is True, ithere are more keys avaialoble 
            to list. To get them, call get_all_keys again with 'marker' set 
            to the name of the last key in the list
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
        data_dict = json.loads(data)

        if "keys" in data_dict:
            result_list = TruncatableList(
                [Key(bucket=self, name=k) for k in data_dict["keys"]]
            ) 
        elif "prefixes" in data_dict:
            result_list = TruncatableList(
                [Prefix(bucket=self, name=p) for p in data_dict["prefixes"]]
            )
        else:
            raise ValueError("Unexpected retruen value %s" % (data_dict, ))

        result_list.truncated = data_dict["truncated"]
        return result_list

    def get_all_multipart_uploads(
        self, max_uploads=1000, key_marker="", upload_id_marker=""
    ):
        """
        max_uploadss
            The maximum number of keys to retrieve

        key_marker
            The retrieve starts on the next key after this one

        upload_id_marker 
            if key_marker is specfied, only include uploads with upload_id
            greater than this value

        return a list of all keys in this collection
        """
        method = "GET"

        http_connection = self.create_http_connection()

        kwargs = {
            "max_uploads" : max_uploads,
        }
        if key_marker != "" and key_marker is not None: 
            kwargs["key_marker"] = key_marker
        if upload_id_marker != "" and upload_id_marker is not None: 
            kwargs["upload_id_marker"] = upload_id_marker

        uri = compute_uri("conjoined/", **kwargs)

        response = http_connection.request(method, uri)
        
        data = response.read()
        http_connection.close()

        data_dict = json.loads(data)
        result_list = TruncatableList()
        for conjoined_dict in data_dict["conjoined_list"]:
            result_list.append(MultiPartUpload(bucket=self, **conjoined_dict)
        )
        result_list.truncated = data_dict["truncated"]

        return result_list

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

    def initiate_multipart_upload(self, key_name):
        """
        key_name
            the key name

        """
        kwargs = {
            "action" : "start"
        }
        # TODO: boto allows meta data here

        method = "POST"
        uri = compute_uri("conjoined", key_name, **kwargs)

        http_connection = self.create_http_connection()

        self._log.info("posting %s" % (uri, ))
        response = http_connection.request(method, uri)
        
        data = response.read()

        http_connection.close()

        result_dict = json.loads(data)
        return result_dict["conjoined_identifier_hex"]


