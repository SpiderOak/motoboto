# -*- coding: utf-8 -*-
"""
resumable_download_handler.py

simulates the boto resumable download handler which originally came from
google gsutil
"""
import logging

class ResumableDownloadHandler(object):
    """
    Handler for resumable downloads

    This class exists for boto compatiblilty. 
    """
    def __init__(self, tracker_file_name=None, num_retries=None):
        """
        tracker_file_name
            path to tracker file

        num_retries
            limit to the number of times we will retry
        """
        self._log = logging.getLogger("ResumeableDownloadHandler")

    def _save_tracker_info(self, key):
        pass

