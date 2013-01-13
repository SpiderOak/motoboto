# -*- coding: utf-8 -*-
"""
init for motoboto
"""
from motoboto.s3_emulator import S3Emulator

def connect_s3(identity=None):
    return S3Emulator(identity)

