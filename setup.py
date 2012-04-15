# -*- coding: utf-8 -*-
"""
setup.py

setup file for motoboto
"""

import sys
from setuptools import setup, find_packages

_name = "motoboto"
_description = "A plugin replacement for the s3 part of boto"
_version = "0.0.0"
_author = "Doug Fort"
_author_email = "dougfort@spideroak.com"
_url = "https://spideroak.com"
_classifiers = [
    "Development Status :: 1 - Planning",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: BSD License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 2.7",
    "Topic :: Software Development :: Libraries",

]
_entry_points = {
    "console_scripts" : [
        "nio_cmd = motoboto.nio_cmd.nio_cmd_main:main",
    ]
}

pre_python27 = (sys.version_info[0] == 2 and sys.version_info[1] < 7)

_requires = (["lumberyard (>=0.1)", ], ["lumberyard (>=0.1)", "unittest2"])[pre_python27]
with open("README.txt") as input_file:
    _long_description = input_file.read()

setup(
    name=_name,
    description=_description,
    long_description=_long_description,
    author=_author,
    author_email=_author_email,
    url=_url,
    packages=find_packages(),
    version=_version,
    classifiers=_classifiers,
    entry_points=_entry_points,
    requires=_requires
)
