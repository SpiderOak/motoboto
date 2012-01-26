# -*- coding: utf-8 -*-
"""
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
    wrap a nimbus.io key to simulate a boto Key object
    """
    def __init__(self, bucket=None, name=None, version_id=None):
        self._log = logging.getLogger("Key")
        self._bucket = bucket
        self._name = name
        self._version_id = None
        self._size = None
        self._metadata = dict()

    def close(self):
        """
        close this key
        """
        self._log.debug("closing")

    def _get_name(self):
        """key name."""
        return self._name

    def _set_name(self, value):
        self._name = value

    name = property(_get_name, _set_name)
    key = property(_get_name, _set_name)

    @property
    def version_id(self):
        return self._version_id

    def __str__(self):
        return self.name

    def _get_size(self):
        """key size."""
        return self._size

    def _repr__(self):
        return "/".join([self._bucket.name, self.name, ])

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
        self, 
        data, 
        replace=True, 
        cb=None, 
        cb_count=10, 
        multipart_id=None,
        part_num=0
    ):
        """
        data
            the string to archive

        replace
            True if existing contents are to be written over. (this **must**
            be True for motoboto)

        cb
            callback function for reporting progress

        cb_count
            number of callbacks to be made during the archvie process

        multipart_id
            identifier of multipart upload

        part_num
            part number of multipart upload

        archive the content of the string into nimbus.io
        """
        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        kwargs = {
            "conjoined_identifier"  : multipart_id,
            "conjoined_part"        : part_num,
            "replace"               : str(replace),
        }
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
        self, 
        file_object, 
        replace=True, 
        cb=None, 
        cb_count=10,
        multipart_id=None,
        part_num=0
    ):
        """
        file_object
            a file-like object opened to the file to be archived. Must support
            read().

        replace
            True if existing contents are to be written over. (this **must**
            be True for motoboto)

        cb
            callback function for reporting progress

        cb_count
            number of callbacks to be made during the archvie process

        multipart_id
            identifier of multipart upload

        part_num
            part number of multipart upload

        archive the content of the file in nimbus.io
        """
        if self._bucket is None:
            raise ValueError("No bucket")
        if self._name is None:
            raise ValueError("No name")

        # 2011-08-07 dougfort -- If they don't want to replace,
        # stop them right here.
        if not replace:
            if self.exists():
                raise KeyError("attempt to inot replace key %r" % (self._name))

        wrapper = None
        if cb is None:
            body = file_object
        else:
            body = ReadReporter(file_object)
            wrapper = ArchiveCallbackWrapper(body, cb, cb_count) 

        kwargs = {
            "conjoined_identifier"  : multipart_id,
            "conjoined_part"        : part_num
        }
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
        cb
            callback function for reporting progress

        cb_count
            number of callbacks to be made during the archvie process

        retrieve the contents from nimbus.io as a string
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
        file_object
            Python file-like object, must support write()

        cb
            callback function for reporting progress

        cb_count
            number of callbacks to be made during the archvie process

        retrieve the contents from nimbus.io to a file
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
        delete this key from the nimbus.io collection
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
        """
        meta_key
            name of the meta item

        meta_value
            value of the meta item

        add a key/value meta item to be associated wiht the key
        """        
        self._metadata[meta_key] = meta_value
        
    def update_metadata(self, meta_dict):
        """
        add a dcitionary of meta itms to be associated with the key
        """
        self._metadata.update(meta_dict)

    def get_metadata(self, meta_key):
        """
        return the meta_value associated with the meta_key
        
        returns None if the meta_key (or the key itself) does not exist.
        """

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

