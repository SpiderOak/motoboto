# -*- coding: utf-8 -*-
"""
multipart.py

"""
import logging
import uuid

from lumberyard.http_util import compute_uri

from motoboto.s3.util import parse_timestamp_repr

class CompleteMultiPartUpload(object):
    """
    Represents a completed MultiPart Upload.
    """
    pass

class MultiPartUpload(object):
    """
    Represents a MultiPart Upload operation.
    """

    def __init__(self, bucket=None, **kwargs):
        self._log = logging.getLogger("MultiPartUpload")
        self._bucket = bucket
        self._conjoined_identifier = kwargs["conjoined_identifier"]
        self.key_name = kwargs["key"]
        self.create_timestamp = parse_timestamp_repr(
            kwargs["create_timestamp"]
        )
        if "abort_timestamp" in kwargs and \
           kwargs["abort_timestamp"] is not None and \
           len(kwargs["abort_timestamp"]) > 0:
            self.abort_timestamp = parse_timestamp_repr(
                kwargs["abort_timestamp"]
            )
        else:
            self.abort_timestamp = None

        if "complete_timestamp" in kwargs and \
           kwargs["complete_timestamp"] is not None and \
           len(kwargs["complete_timestamp"]) > 0:
            self.complete_timestamp = parse_timestamp_repr(
                kwargs["complete_timestamp"]
            )
        else:
            self.complete_timestamp = None

        if "delete_timestamp" in kwargs and \
           kwargs["delete_timestamp"] is not None and \
           len(kwargs["delete_timestamp"]) > 0:
            self.delete_timestamp = parse_timestamp_repr(
                kwargs["delete_timestamp"]
            )
        else:
            self.delete_timestamp = None

    @property
    def id(self):
        return self._conjoined_identifier

    def cancel_upload(self):
        """
        Cancels a MultiPart Upload operation. The storage consumed by any 
        previously uploaded parts will be freed.
        """
        kwargs = {
            "action"                : "abort",
            "conjoined_identifier"  : self._conjoined_identifier,
        }

        method = "POST"
        uri = compute_uri("conjoined", self.key_name, **kwargs)

        http_connection = self._bucket.create_http_connection()

        self._log.info("posting %s" % (uri, ))
        response = http_connection.request(method, uri)
        
        response.read()

        http_connection.close()

    def complete_upload(self):
        """
        Complete the MultiPart Upload operation. 
        
        This method should be called when all parts of the file have been 
        successfully uploaded.
        """
        kwargs = {
            "action"                : "finish",
            "conjoined_identifier"  : self._conjoined_identifier,
        }

        method = "POST"
        uri = compute_uri("conjoined", self.key_name, **kwargs)

        http_connection = self._bucket.create_http_connection()

        self._log.info("posting %s" % (uri, ))
        response = http_connection.request(method, uri)
        
        response.read()

        http_connection.close()


    def get_all_parts(self, max_parts=None, part_number_marker=None):
        """
        Return the uploaded parts of this MultiPart Upload.
        """

    def upload_part_from_file(
        self, fp, part_num, replace=True, cb=None, num_cb=10
    ):
        """
        fp
            file pointer to filke being uploaded

        part_num
            number of this part

        replace
            True if existing contents are to be written over. (this **must**
            be True for motoboto)

        cb
            callback function for reporting progress

        cb_count
            number of callbacks to be made during the archvie process

        Upload a part of this MultiPart Upload.
        """
        key = self._bucket.get_key(self.key_name)
        key.set_contents_from_file(
            fp, 
            replace=replace, 
            cb=cb, 
            cb_count=num_cb,
            multipart_id=self._conjoined_identifier,
            part_num=part_num
        )

class Part(object):
    """
    Represents a single part in a MultiPart upload. Attributes include:
    """
    def __init__(self, bucket=None):
        """
        part_number
            The integer part number

        last_modified
            The last modified date of this part

        etag
            The MD5 hash of this part
            
        size
            The size, in bytes, of this part
        """        

def part_lister(mpupload, part_number_marker=None):
    """
    A generator function for listing parts of a multipart upload.
    """


