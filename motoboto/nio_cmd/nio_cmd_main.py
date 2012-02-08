# -*- coding: utf-8 -*-
"""
nio_cmd main.py nimbus.io commandline program



"""

import logging
import sys

import motoboto

from motoboto.nio_cmd.argument_parser import parse_arguments, usage, \
        cmd_list_all_buckets, \
        cmd_list_bucket
from motoboto.nio_cmd.bucket_lister import list_all_buckets, list_bucket

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
    cmd_list_all_buckets    : list_all_buckets,
    cmd_list_bucket         : list_bucket,
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

