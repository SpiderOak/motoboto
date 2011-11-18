# -*- coding: utf-8 -*-
"""
key.py

simulate a boto Key object
"""
import json
import logging

from lumberyard.http_connection import LumberyardHTTPError
from lumberyard.http_util import compute_uri, meta_prefix
from lumberyard.read_reporter import ReadReporter

from motoboto.s3.archive_callback_wrapper import ArchiveCallbackWrapper
from motoboto.s3.retrieve_callback_wrapper import NullCallbackWrapper, \
        RetrieveCallbackWrapper

_read_buffer_size = 64 * 1024

class Key(object):
    """
    simulate a boto Key object
    """
    def __init__(self, bucket=None, name=None):
        self._log = logging.getLogger("Key")
        self._bucket = bucket
        self._name = name
        self._size = None
        self._metadata = dict()

    def close(self):
        self._log.debug("closing")

    def _get_name(self):
        """key name."""
        return self._name

    def _set_name(self, value):
        self._name = value

    name = property(_get_name, _set_name)
    key = property(_get_name, _set_name)

    def _get_size(self):
        """key size."""
        return self._size

    def _set_size(self, value):
        self._size = value

    size = property(_get_size, _set_size)

    def exists(self):
        """
        return True if we can HEAD the key
        """  
        found = False

        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        method = "HEAD"
        uri = compute_uri("data", self._name)
        
        http_connection = self._bucket.create_http_connection()

        self._log.info("requesting HEAD %s" % (uri, ))
        try:
            response = http_connection.request(method, uri, body=None)
        except LumberyardHTTPError, instance:
            if instance.status == 404: # not found
                pass
            else:
                self._log.error(str(instance))
                http_connection.close()
                raise
        else:
            found = True
        
        if found:
            response.read()
            
        http_connection.close()

        return found

    def set_contents_from_string(
        self, data, replace=True, cb=None, cb_count=10
    ):
        """
        store the content of the string in the lumberyard
        """
        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        # 2011-08-07 dougfort -- If they don't want to replace,
        # stop them right here.
        if not replace:
            if self.exists():
                raise KeyError("attempt to replace key %r" % (self._name))

        kwargs = {}
        for meta_key, meta_value in self._metadata.items():
            kwargs["".join([meta_prefix, meta_key])] = meta_value

        method = "POST"
        uri = compute_uri("data", self._name, **kwargs)

        http_connection = self._bucket.create_http_connection()

        self._log.info("posting %s" % (uri, ))
        response = http_connection.request(method, uri, body=data)
        
        response.read()

        http_connection.close()

    def set_contents_from_file(
        self, file_object, replace=True, cb=None, cb_count=10
    ):
        """
        store the content of the file in lumberyard
        """
        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        # 2011-08-07 dougfort -- If they don't want to replace,
        # stop them right here.
        if not replace:
            if self.exists():
                raise KeyError("attempt to replace key %r" % (self._name))

        wrapper = None
        if cb is None:
            body = file_object
        else:
            body = ReadReporter(file_object)
            wrapper = ArchiveCallbackWrapper(body, cb, cb_count) 

        kwargs = {}
        for meta_key, meta_value in self._metadata:
            kwargs["".join([meta_prefix, meta_key])] = meta_value

        method = "POST"
        uri = compute_uri("data", self._name, **kwargs)

        http_connection = self._bucket.create_http_connection()

        self._log.info("requesting POST %s" % (uri, ))
        response = http_connection.request(method, uri, body=body)
        
        response.read()

    def get_contents_as_string(self, cb=None, cb_count=10):
        """
        return the contents from lumberyard as a string
        """
        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        method = "GET"
        uri = compute_uri("data", self._name)

        http_connection = self._bucket.create_http_connection()

        self._log.info("requesting GET %s" % (uri, ))
        response = http_connection.request(method, uri, body=None)
        
        body_list = list()
        while True:
            data = response.read(_read_buffer_size)
            if len(data) == 0:
                break
            body_list.append(data)

        http_connection.close()

        return "".join(body_list)

    def get_contents_to_file(self, file_object, cb=None, cb_count=10):
        """
        return the contents from lumberyard to a file
        """
        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        method = "GET"
        uri = compute_uri("data", self._name)

        http_connection = self._bucket.create_http_connection()

        self._log.info("requesting GET %s" % (uri, ))
        response = http_connection.request(method, uri, body=None)

        if cb is None:
            reporter = NullCallbackWrapper()
        else:
            reporter = RetrieveCallbackWrapper(self.size, cb, cb_count) 
        
        self._log.info("reading response")
        reporter.start()
        while True:
            data = response.read(_read_buffer_size)
            bytes_read = len(data)
            if bytes_read == 0:
                break
            file_object.write(data)
            reporter.bytes_written(bytes_read)
        reporter.finish()
        http_connection.close()

    def delete(self):
        """
        delete this key from the system
        """
        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        method = "DELETE"
        uri = compute_uri("data", self._name)

        http_connection = self._bucket.create_http_connection()

        self._log.info("requesting DELETE %s" % (uri, ))
        response = http_connection.request(method, uri, body=None)
        
        response.read()
        http_connection.close()

    def set_metadata(self, meta_key, meta_value):
        self._metadata[meta_key] = meta_value
        
    def update_metadata(self, meta_dict):
        self._metadata.update(meta_dict)

    def get_metadata(self, meta_key):

        # If we have it local, pass it on
        if meta_key in self._metadata:
            return self._metadata[meta_key]

        method = "GET"

        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        http_connection = self._bucket.create_http_connection()

        kwargs = {
            "action"            : "meta", 
        }

        uri = compute_uri("data", self._name, **kwargs)
        
        self._log.info("requesting GET %s" % (uri, ))
        try:
            response = http_connection.request(method, uri, body=None)
        except LumberyardHTTPError, instance:
            http_connection.close()

            if instance.status == 404: # not found
                self._log.warn("key not found retrieving meta")
                return None

            self._log.error(str(instance))
            raise
        
        data = response.read()

        http_connection.close()

        self.update_metadata(json.loads(data))

        return self._metadata.get(meta_key)

