#!/usr/bin/env python3

import pytest
from webtest import TestApp
from pytest_postgresql import factories
from webservice import main, views, model


postgresql_my_proc = factories.postgresql_noproc(host="localhost", port="5432", password="password")
postgres_with_schema = factories.postgresql('postgresql_my_proc', dbname="test", load=['./eidastats_schema.sql'])

def test_postgres(postgres_with_schema):
    """
    Check main postgres fixture
    """

    with postgres_with_schema.cursor() as cur:
        cur.execute("select * from dataselect_stats limit 100")
        cur.fetchall()


@pytest.fixture
def app():
    appl = main({})
    testapp = TestApp(appl)
    return testapp


def test_wrong_parameter(app):
    """
    Check request with invalid parameter given
    """

    response = app.get('/dataselect/query?wrong=stg', status=400)

    assert 'Invalid parameter' in str(response.body)


def test_wrong_parameter_value_date(app):
    """
    Check request with invalid value of date parameter given
    """

    response = app.get('/dataselect/query?start=stg', status=400)

    assert 'Unsupported value for parameter' in str(response.body)


def test_wrong_parameter_value_aggregate(app):
    """
    Check request with invalid value of aggregate_on parameter given
    """

    response = app.get('/dataselect/query?aggregate_on=stg', status=400)

    assert 'Unsupported value for parameter' in str(response.body)


def test_wrong_parameter_value_format(app):
    """
    Check request with invalid value of format parameter given
    """

    response = app.get('/dataselect/query?format=stg', status=400)

    assert 'Unsupported value for parameter' in str(response.body)


def test_correct_request(app):
    """
    Check correct request
    """

    response = app.get('/dataselect/query?start=2021-05&country=GR&network=HL&aggregate_on=month,station,network,datacenter', status=200)

    assert 'version' in str(response.body)
