# -*- coding: utf-8 -*-
"""
collection_lister.py

list the keys in a collection
"""

def list_all_buckets(motoboto_connection):
    """
    list all buckets for the user
    """
    for bucket in motoboto_connection.get_all_buckets():
        print bucket.name

def list_bucket(motoboto_connection, bucket_name):
    """
    list all keys in the bucket
    """
    bucket = motoboto_connection.get_bucket(bucket_name)
    for key in bucket.list():
        print key.name

