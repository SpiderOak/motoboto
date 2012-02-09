# -*- coding: utf-8 -*-
"""
argument_parser.py

parse commandline arguments
"""
import sys

cmd_list_all_buckets = "list-all_buckets"
cmd_list_bucket = "list-bucket"
cmd_remove_key = "remove-key"
cmd_copy_file_to_nimbusio = "copy-file-to-nimbusio"
cmd_copy_stdin_to_nimbusio = "copy-stdin-to-nimbusio"
cmd_copy_nimbusio_to_file = "copy-nimbusio-to-file"
cmd_copy_nimbusio_to_nimbusio = "copy-nimbusio-to-nimbusio"
cmd_copy_s3_to_nimbusio = "copy-s3-to-nimbusio"
cmd_copy_nimbusio_to_s3 = "copy-nimbusio-to-s3"
cmd_move_s3_to_nimbusio = "move-s3-to-nimbusio"

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

# copy a key from one nimbus.io location to another
nio_cmd cp nimbus.io://bucket_name1/key_name1 nimbusio://bucket_name2/key_name2 

# copy a key from s3 to nimbus.io
nio_cmd cp s3://bucket_name/key_name nimbusio://bucket_name/key_name 

# copy a key from nimbus.io to s3
nio_cmd cp nimbusio://bucket_name/key_name s3://bucket_name/key_name  

# move a key from s3 to nimbus.io
nio_cmd mv s3://bucket_name/key_name nimbusio://bucket_name/key_name 
"""

_separator = "/"

_nimbusio_file_type = "nimbus.io://"
_s3_file_type = "s3://"
_stdin_file_type = "-"

def _parse_bucket_path(text):
    result = text.split(_separator, 1)
    assert len(result) == 2
    (bucket_name, key_name, ) = result
    return (bucket_name, key_name, )

def _parse_nimbusio_file_type(text):
    """
    accept "nimbus.io://<bucket-name>/key-name"
    return (bucket_name, key_name, )
    """
    assert text.startswith(_nimbusio_file_type)
    return _parse_bucket_path(text[len(_nimbusio_file_type):])

def _parse_s3_file_type(text):
    """
    accept "s3://<bucket-name>/key-name"
    return (bucket_name, key_name, )
    """
    assert text.startswith(_s3_file_type)
    return _parse_bucket_path(text[len(_s3_file_type):])

def _parse_ls(args):
    if len(args) == 0:
        return (cmd_list_all_buckets, args, )

    if len(args) == 1:
        return (cmd_list_bucket, args, )

    raise ValueError("must ls with no arguments or with a single bucket name")

def _parse_rm(args):
    if len(args) == 2:
        return (cmd_remove_key, args, )

    raise ValueError("Expecting rm <bucket-name> <key-name> '{0}'".format(
        args
    ))

def _parse_cp(args):
    if len(args) != 2:
        raise ValueError("Expecting cp <source> <dest> '{0}'".format(args))

    source, dest = args

    if source.startswith(_nimbusio_file_type) and \
       dest.startswith(_nimbusio_file_type):
        source_bucket, source_key = _parse_nimbusio_file_type(source)
        dest_bucket, dest_key = _parse_nimbusio_file_type(dest)
        return (cmd_copy_nimbusio_to_nimbusio, 
                [source_bucket, source_key, dest_bucket, dest_key])

    if source.startswith(_s3_file_type) and \
       dest.startswith(_nimbusio_file_type):
        source_bucket, source_key = _parse_s3_file_type(source)
        dest_bucket, dest_key = _parse_nimbusio_file_type(dest)
        return (cmd_copy_s3_to_nimbusio, 
                [source_bucket, source_key, dest_bucket, dest_key])

    if source.startswith(_nimbusio_file_type) and \
       dest.startswith(_s3_file_type):
        source_bucket, source_key = _parse_nimbusio_file_type(source)
        dest_bucket, dest_key = _parse_s3_file_type(dest)
        return (cmd_copy_nimbusio_to_s3, 
                [source_bucket, source_key, dest_bucket, dest_key])

    if source == _stdin_file_type and dest.startswith(_nimbusio_file_type):
        dest_bucket, dest_key = _parse_nimbusio_file_type(dest)
        return (cmd_copy_stdin_to_nimbusio, [dest_bucket, dest_key, ])

    if dest.startswith(_nimbusio_file_type):
        dest_bucket, dest_key = _parse_nimbusio_file_type(dest)
        return (cmd_copy_file_to_nimbusio, [source, dest_bucket, dest_key])

    if source.startswith(_nimbusio_file_type):
        source_bucket, source_key = _parse_nimbusio_file_type(source)
        return (cmd_copy_nimbusio_to_file, [source_bucket, source_key, dest])

    raise ValueError("Unparsable cp arguments {0}".format(args)) 

def _parse_mv(args):
    if len(args) != 2:
        raise ValueError("Expecting mv <source> <dest> '{0}'".format(args))

    source, dest = args

    if source.startswith(_s3_file_type) and \
       dest.startswith(_nimbusio_file_type):
        source_bucket, source_key = _parse_s3_file_type(source)
        dest_bucket, dest_key = _parse_nimbusio_file_type(dest)
        return (cmd_move_s3_to_nimbusio, 
                [source_bucket, source_key, dest_bucket, dest_key])

    raise ValueError("Unparsable mv arguments {0}".format(args)) 

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

