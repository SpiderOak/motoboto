# -*- coding: utf-8 -*-
"""
utility routines for unit tests
"""
import logging
import os
import os.path

_tmp_path = os.environ.get("TEMP", "/tmp")
test_dir_path = os.path.join(_tmp_path, "test_s3_replacement")

def initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    
    logging.root.addHandler(console)

    logging.root.setLevel(logging.DEBUG)

