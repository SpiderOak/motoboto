# -*- coding: utf-8 -*-
"""
argument_parser.py

parse commandline arguments
"""
import sys

cmd_list_all_buckets = "list-all_buckets"
cmd_list_bucket = "list-bucket"

usage = """
# list keys in bucket 
nio_cmd ls bucket_name 

# delete a key in a bucket
nio_cmd rm bucket_name key_name  

# copy the local file filename.ext to key_name in bucket_name 
# (use a filename of - to copy stdin)
nio_cmd cp filename.ext nimbus.io://bucket_name/key_name  

# copy the contents of key_name in bucket_name mylocalfile.ext
nio_cmd cp nimbus.io://bucket_name/key_name mylocalfile.ext 

# copy the contents of key_name in bucket_name to the local file
# mylocalfile.ext
nio_cmd cp nimbus.io://bucket_name/key_name mylocalfile.ext 

# copy a key from one nimbus.io location to another
nio_cmd cp nimbus.io://bucket_name1/key_name1 nimbusio://bucket_name2/key_name2 

# copy a key from s3 to nimbus.io
nio_cmd cp s3://bucket_name/key_name nimbusio://bucket_name/key_name 

# copy a key from nimbus.io to s3
nio_cmd cp nimbusio://bucket_name/key_name s3://bucket_name/key_name  

# move a key from s3 to nimbus.io
nio_cmd mv s3://bucket_name/key_name nimbusio://bucket_name/key_name 
"""

def _parse_ls(args):
    if len(args) == 0:
        return (cmd_list_all_buckets, args, )

    if len(args) == 1:
        return (cmd_list_bucket, args, )

    raise ValueError("must ls with no arguments or with a single bucket name")

def _parse_rm(args):
    pass

def _parse_cp(args):
    pass

def _parse_mv(args):
    pass

_parse_dispatch_table = {
    "ls" : _parse_ls,
    "rm" : _parse_rm,
    "cp" : _parse_cp,
    "mv" : _parse_mv,
}

def parse_arguments():
    """
    parse the command line 
    """
    if len(sys.argv) < 2:
        raise ValueError("must specify commandline argument")

    try:
        return _parse_dispatch_table[sys.argv[1]](sys.argv[2:])
    except KeyError:
        raise ValueError("unknown command '{0}'".format(sys.argv[1]))

