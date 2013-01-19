# -*- coding: utf-8 -*-
"""
s3_emulator.py

Emulate the functions of the object returned by boto.connect_s3
"""
from datetime import datetime
try:
    from httplib import CREATED
except ImportError:
    from http.client import CREATED
import json
import logging
import sys

from lumberyard.http_connection import HTTPConnection, LumberyardHTTPError
from lumberyard.http_util import compute_default_hostname, \
        compute_default_collection_name, \
        compute_reserved_collection_name, \
        compute_uri

from motoboto.identity import load_identity_from_environment, \
        load_identity_from_file
from motoboto.s3.bucket import Bucket

class S3Emulator(object):
    """
    Emulate the functions of the object returned by boto.connect_s3

    if identity is None
    * first look for environment variables
    * then look for an identity fiel in a standard location
    """
    def __init__(self, identity=None):
        self._log = logging.getLogger("S3Emulator")

        if identity is not None:
            self._identity = identity
        else:
            identity = load_identity_from_environment()
            if identity is not None:
                self._identity = identity
            else:
                identity = load_identity_from_file()
                if identity is not None:
                    self._identity = identity
                else:
                    raise ValueError(
                        "You must specify identity in environment or file"
                    )

        self._default_bucket = Bucket(
            self._identity, 
            compute_default_collection_name(self._identity.user_name)
        )

    @property
    def default_bucket(self):
        return self._default_bucket

    def close(self):
        """
        close connection to motoboto
        """
        self._log.debug("closing")

    def get_bucket(self, bucket_name):
        """
        get the contents of an existing nimbus.io collection, 
        similar to an s3 bucket
        """
        return Bucket(self._identity, bucket_name)

    def create_bucket(self, bucket_name, access_control=None):
        """
        create a nimbus.io collection, similar to an s3 bucket

        nimbus.io organizes the objects that you store into collections. Every 
        nimbus.io key is a member of a collection. For efficient access to your 
        data nimbus.io uses the collection name as part of the `hostname`_.

        For example, to act on objects in the collection 
        ``my-temperature-readings``, your HTTP query would be directed to 
        hostname ``my-temperature-readings.nimbus.io``

        This approach requires some restrictions on your collection names:

        * collection names must be **unique**: you cannot use a colection name 
            that someone else is already using.

        * Internet standards mandate that collection names may contain only 

          * the ASCII letters **a** through **z** (case-insensitive), 
          * the digits **0** through **9**, 
          * the hyphen (**-**).

        * collection names must be between 1 and 63 characters long

        nimbus.io gives you a default collection name of 
        ``dd-<your user name>``

        you don't need to create your default collection
        you cannot delete your default collection

        To reduce the inconvenience of creating a unique collection name, 
        nimbus.io provides a facility for creating guaranteed unique names of 
        the form ``rr-<your user-name>-<collection name>``. Of course, this 
        must comply with the restrictons mentioned above.

        .. _hostname: http://en.wikipedia.org/wiki/Hostname
        """
        method = "POST"

        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._identity.user_name,
            self._identity.auth_key,
            self._identity.auth_key_id
        )
        uri = compute_uri(
            "/".join(["customers", self._identity.user_name, "collections"]), 
            action="create",
            name=bucket_name
        )

        body = None
        headers = dict()
        if access_control is not None:
            body = access_control
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = len(body)

        self._log.info("requesting {0} {1}".format(uri, headers))
        try:
            response = http_connection.request(method, 
                                               uri, 
                                               body=body,
                                               headers=headers,
                                               expected_status=CREATED)
        except LumberyardHTTPError:
            instance = sys.exc_info()[1]
            self._log.error(str(instance))
            http_connection.close()
            raise
        
        response.read()
        http_connection.close()

        return Bucket(self._identity, bucket_name)

    def create_unique_bucket(self, access_control=None):
        """
        create a nimbus.io collection, similar to an s3 bucket
        this bucket will have a unique name, not duplicating any existing bucket

        The bucket name is be based on the current time. This will fail if
        called within a short time.

        See the documentation for ``create_bucket`` for more detail
        """
        current_time = datetime.utcnow()
        time_string = current_time.strftime("%Y%m%d%H%M%S%f")
        bucket_name = compute_reserved_collection_name(self._identity.user_name,
                                                       time_string)

        return self.create_bucket(bucket_name, access_control)
 
    def get_all_buckets(self):
        """
        List all collections for the user

        returns a list of motoboto.s3.Bucket objects
        """
        method = "GET"

        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._identity.user_name,
            self._identity.auth_key,
            self._identity.auth_key_id
        )
        uri = compute_uri(
            "/".join(["customers", self._identity.user_name, "collections"]), 
        )

        self._log.info("requesting {0}".format(uri))
        try:
            response = http_connection.request(method, uri, body=None)
        except LumberyardHTTPError:
            instance = sys.exc_info()[1]
            self._log.error(str(instance))
            http_connection.close()
            raise
        
        self._log.info("reading response")
        data = response.read()
        http_connection.close()
        collection_list = json.loads(data.decode("utf-8"))

        bucket_list = list()
        for collection_dict in collection_list:
            bucket = Bucket(
                self._identity, 
                collection_dict["name"], 
                versioning=collection_dict["versioning"]
            )
            bucket_list.append(bucket)
        return bucket_list

    def delete_bucket(self, bucket_name):
        """
        remove (an empty) bucket from nimbus.io

        This operation will fail if the collection contains any active keys.

        When tis operaton succeeds, the colection/bucket name is available for
        re-use.
        """
        method = "DELETE"

        http_connection = HTTPConnection(
            compute_default_hostname(),
            self._identity.user_name,
            self._identity.auth_key,
            self._identity.auth_key_id
        )

        if bucket_name.startswith("/"):
            bucket_name = bucket_name[1:]
        uri = compute_uri(
            "/".join([
                "customers", 
                self._identity.user_name, 
                "collections",
                bucket_name
            ]), 
        )

        self._log.info("requesting {0}".format(uri))
        try:
            response = http_connection.request(method, uri, body=None)
        except LumberyardHTTPError:
            instance = sys.exc_info()[1]
            self._log.error(str(instance))
            http_connection.close()
            raise
        
        response.read()
        http_connection.close()

