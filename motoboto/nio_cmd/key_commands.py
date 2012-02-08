# -*- coding: utf-8 -*-
"""
key_commands.py
"""

from cStringIO import StringIO
import sys

def remove_key(motoboto_connection, bucket_name, key_name):
    """
    remove a key from the bucket
    """
    bucket = motoboto_connection.get_bucket(bucket_name)
    key = bucket.get_key(key_name)
    key.delete()

def copy_file_to_nimbusio(
    motoboto_connection, source_path, dest_bucket_name, dest_key_name
):
    dest_bucket = motoboto_connection.get_bucket(dest_bucket_name)
    dest_key = dest_bucket.get_key(dest_key_name)
    with open(source_path, "rb") as source_file:
        dest_key.set_contents_from_file(source_file)

def copy_stdin_to_nimbusio(
    motoboto_connection, dest_bucket_name, dest_key_name
):
    # we can feed stdin directly to the key because we don't know the
    # content length.
    dest_bucket = motoboto_connection.get_bucket(dest_bucket_name)
    dest_key = dest_bucket.get_key(dest_key_name)
    dest_key.set_contents_from_string(sys.stdin.read())

def copy_nimbusio_to_file(
    motoboto_connection, source_bucket_name, source_key_name, dest_path
):
    source_bucket = motoboto_connection.get_bucket(source_bucket_name)
    source_key = source_bucket.get_key(source_key_name)
    with open(dest_path, "wb") as dest_file:
        source_key.get_contents_to_file(dest_file)

