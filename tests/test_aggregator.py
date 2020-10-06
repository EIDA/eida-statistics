#!/usr/bin/env python3

from aggregator import eida_log_aggregation

def test_whatever():
    assert True

def test_parse_file():
    stats = eida_log_aggregation.parse_file('/home/schaeffj/factory/vcs/eida-stats/tests/files/test.log.bz2')
    for key,stat in stats.items():
        print(stat.info())
