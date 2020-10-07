#!/usr/bin/env python3

from aggregator import eida_log_aggregation
import datetime
import pytest

def test_whatever():
    assert True

def test_parse_file():
    stats = eida_log_aggregation.parse_file('/home/schaeffj/factory/vcs/eida-stats/tests/files/test.log.bz2')
    for key,stat in stats.items():
        print(stat.info())

def test_end_of_week_valid_dates():
    assert eida_log_aggregation.end_of_week(datetime.date(2020,10,7)) == datetime.date(2020,10,11)
    assert eida_log_aggregation.end_of_week(datetime.date(2020,10,11)) == datetime.date(2020,10,11)
    assert eida_log_aggregation.end_of_week(datetime.date(2020,9,30)) == datetime.date(2020,10,4)

def test_end_of_week_invalid_date():
    with pytest.raises(TypeError):
        eida_log_aggregation.end_of_week("2020-09-30")
