#!/usr/bin/env python3

import pytest
from webtest import TestApp
from pytest_postgresql import factories
from ws_eidastats import main, views_main, model


postgresql_my_proc = factories.postgresql_noproc(host="localhost", port="5432", password="password")
postgres_with_schema = factories.postgresql('postgresql_my_proc', dbname="test", load=['./tests/eidastats_schema.sql'])

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

    response = app.get('/dataselect/public?start=2022-01&wrong=stg', status=400)

    assert 'Invalid parameter' in str(response.body)


def test_wrong_parameter_value_date(app):
    """
    Check request with invalid value of date parameter given
    """

    response = app.get('/dataselect/public?start=stg', status=400)

    assert 'Unsupported value for parameter' in str(response.body)


def test_wrong_parameter_value_level(app):
    """
    Check request with invalid value of aggregate_on parameter given
    """

    response = app.get('/dataselect/public?start=2022-01&level=stg', status=400)

    assert 'Unsupported value for parameter' in str(response.body)


def test_wrong_parameter_value_format(app):
    """
    Check request with invalid value of format parameter given
    """

    response = app.get('/dataselect/public?start=2022-01&format=stg', status=400)

    assert 'Unsupported value for parameter' in str(response.body)


def test_wrong_parameter_value_details(app):
    """
    Check request with invalid value of format parameter given
    """

    response = app.get('/dataselect/public?start=2022-01&details=stg', status=400)

    assert 'Unsupported value for parameter' in str(response.body)


def test_correct_public_request(app):
    """
    Check correct public request
    """

    response = app.get('/dataselect/public?start=2021-05&country=GR&level=network&details=month', status=200)

    assert 'version' in str(response.body)


def test_invalid_token(app):
    """
    Check invalid token provided
    """

    with open('./tests/invalid_token', 'rb') as file:
        file_contents = file.read()
    response = app.post('/dataselect/restricted?start=2021-05&country=GR&aggregate_on=month,station,network,datacenter', content_type='application/octet-stream',
                params=file_contents, status=401)

    assert 'Invalid token' in str(response.body)


def test_no_token(app):
    """
    Check no token provided
    """

    response = app.post('/dataselect/raw?start=2021-05&country=GR', status=401)

    assert 'no token' in str(response.body)


def test_method_not_allowed(app):
    """
    Check GET method not allowed
    """

    response = app.get('/dataselect/restricted?start=2021-05&country=GR', status=405)

    assert 'Not Allowed' in str(response.body)
