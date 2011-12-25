# -*- coding: utf-8 -*-
"""
util.py

utility functions used by motoboto
"""
from datetime import datetime
import re

# datetime.datetime(2011, 6, 30, 13, 52, 34, 720271)
_timestamp_repr_re = re.compile(r"""
^datetime.datetime\(
(?P<year>\d{4})         #year
,\s
(?P<month>\d{1,2})      #month
,\s
(?P<day>\d{1,2})        #day
,\s
(?P<hour>\d{1,2})       #hour
,\s
(?P<minute>\d{1,2})     #minute
,\s
(?P<second>\d{1,2})     #second
,\s
(?P<microsecond>\d+)    #microsecond
\)$
""", re.VERBOSE)


def parse_timestamp_repr(timestamp_repr):
    """
    We can't send a timestamp pbject over JSON, so we send the repr
    and parse that to re-create the object
    """
    match_object = _timestamp_repr_re.match(timestamp_repr)
    if match_object is None:
        raise ValueError("unparsable timestamp '%s'" % (timestamp_repr, ))

    timestamp = datetime(
        year=int(match_object.group("year")),
        month=int(match_object.group("month")),
        day=int(match_object.group("day")),
        hour=int(match_object.group("hour")),
        minute=int(match_object.group("minute")),
        second=int(match_object.group("second")),
        microsecond=int(match_object.group("microsecond"))
    )

    return timestamp

