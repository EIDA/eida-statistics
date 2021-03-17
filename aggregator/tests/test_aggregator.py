#!/usr/bin/env python3

from aggregator.aggregator import EidaStatistic, StatCollection
import datetime
import pytest

def test_whatever():
    assert True

def test_parse_bz2_file():
    statcol = StatCollection()
    statcol.parse_file('./tests/files/test.log.bz2')
    for k,stat in statcol._statistics.items():
        print(stat.info())

def test_parse_plain_file():
    statcol = StatCollection()
    statcol.parse_file('./tests/files/test.log')
    for k,stat in statcol._statistics.items():
        print(stat.info())

def test_end_of_week_valid_dates():
    teststat=EidaStatistic(datetime.date(2020,10,7))
    assert teststat._shift_to_begin_of_month() == datetime.date(2020,10,1)

