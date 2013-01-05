# -*- coding: utf-8 -*-
"""
test_all.py

run all unit tests
"""
import os
import re
import unittest

from tests.test_util import initialize_logging

_test_re = re.compile("test_.+?\.py$", re.IGNORECASE)
_exclude_list = ["test_upload_interval.py", ]
_not_excluded_file = lambda f: not f in _exclude_list 
_filename_to_module = lambda f: os.path.splitext(f)[0]
_load = unittest.defaultTestLoader.loadTestsFromModule  

def _regression_test():
    """
    find all unit tests in a directory.
    This is adapted from Mark Pilgrim's 'Dive Into Python'
    """    
    path = "."
    files = os.listdir(path)                               
    files = filter(_test_re.search, files)                     
    files = filter(_not_excluded_file, files)                     
    module_names = map(_filename_to_module, files)         
    modules = map(__import__, module_names)                 
    return unittest.TestSuite(map(_load, modules)) 

if __name__ == "__main__":
    initialize_logging()
    test_suite = _regression_test()
    unittest.TextTestRunner(verbosity=2).run(test_suite)
