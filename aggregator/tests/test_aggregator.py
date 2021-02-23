#!/usr/bin/env python3

from aggregator import aggregator
import datetime
import pytest

def test_whatever():
    assert True

def test_parse_bz2_file():
    stats = aggregator.parse_file('./tests/files/test.log.bz2')
    for key,stat in stats.items():
        print(stat.info())

def test_parse_plain_file():
    stats = aggregator.parse_file('./tests/files/test.log')
    for key,stat in stats.items():
        print(stat.info())

def test_end_of_week_valid_dates():
    assert aggregator.shift_to_begin_of_month(datetime.date(2020,10,7)) == datetime.date(2020,10,1)

def test_end_of_week_invalid_date():
    with pytest.raises(TypeError):
        aggregator.shift_to_begin_of_month("2020-09-30")
