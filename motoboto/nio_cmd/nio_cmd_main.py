# -*- coding: utf-8 -*-
"""
nio_cmd main.py nimbus.io commandline program



"""

import logging
import sys

import motoboto

from motoboto.nio_cmd.argument_parser import parse_arguments, usage, \
        cmd_create_bucket, \
        cmd_list_all_buckets, \
        cmd_list_bucket, \
        cmd_remove_key, \
        cmd_copy_file_to_nimbusio, \
        cmd_copy_stdin_to_nimbusio, \
        cmd_copy_nimbusio_to_file, \
        cmd_copy_nimbusio_to_nimbusio, \
        cmd_copy_s3_to_nimbusio, \
        cmd_copy_nimbusio_to_s3, \
        cmd_move_s3_to_nimbusio
from motoboto.nio_cmd.bucket_lister import create_bucket, \
        list_all_buckets, \
        list_bucket
from motoboto.nio_cmd.key_commands import remove_key, \
        copy_file_to_nimbusio, \
        copy_stdin_to_nimbusio, \
        copy_nimbusio_to_file, \
        copy_nimbusio_to_nimbusio, \
        copy_s3_to_nimbusio, \
        copy_nimbusio_to_s3, \
        move_s3_to_nimbusio

_log_format = '%(asctime)s %(name)-12s: %(levelname)-8s %(message)s'

def _initialize_logging():
    """
    Initialize logging to stderr
    Intended for reporting errors
    """
    log_level = logging.WARN
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(log_level)
    formatter = logging.Formatter(_log_format)
    formatter.datefmt = '%H:%M:%S'
    console.setFormatter(formatter)
    logging.root.addHandler(console)
    logging.root.setLevel(log_level)

_dispatch_table = {
    cmd_create_bucket               : create_bucket,
    cmd_list_all_buckets            : list_all_buckets,
    cmd_list_bucket                 : list_bucket,
    cmd_remove_key                  : remove_key,
    cmd_copy_file_to_nimbusio       : copy_file_to_nimbusio,
    cmd_copy_stdin_to_nimbusio      : copy_stdin_to_nimbusio,
    cmd_copy_nimbusio_to_file       : copy_nimbusio_to_file,
    cmd_copy_nimbusio_to_nimbusio   : copy_nimbusio_to_nimbusio,
    cmd_copy_s3_to_nimbusio         : copy_s3_to_nimbusio,
    cmd_copy_nimbusio_to_s3         : copy_nimbusio_to_s3,
    cmd_move_s3_to_nimbusio         : move_s3_to_nimbusio,
}

def main():
    """
    main program entry point
    returns 0 for normal termination
    """
    _initialize_logging()
    log = logging.getLogger("main")
    log.debug("program starts")

    try:
        motoboto_connection = motoboto.connect_s3()
    except Exception:
        log.exception("Unable to connect to motoboto")
        return 1

    try:
        command, args = parse_arguments()
    except ValueError, instance:
        motoboto_connection.close()
        log.error("Invalid arguments: {0}".format(instance))
        print usage
        return 2

    full_args = [motoboto_connection, ]
    full_args.extend(args)

    try:
        _dispatch_table[command](*full_args)
    except Exception:
        motoboto_connection.close()
        log.exception("{0} {1}".format(command, full_args))
        return 3

    motoboto_connection.close()
    log.debug("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

