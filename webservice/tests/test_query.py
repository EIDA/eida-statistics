#!/usr/bin/env python3

import pytest
from pytest_postgresql import factories
import sys
sys.path.append('../webservice')
import query
import model


postgresql_my_proc = factories.postgresql_noproc(host="localhost", port="5432", password="password")
postgres_with_schema = factories.postgresql('postgresql_my_proc', dbname="test", load=['./eidastats_schema.sql'])


@pytest.fixture()
def app():
    query.app.config['TESTING'] = True
    yield query.app


@pytest.fixture()
def client(app):
    return query.app.test_client()


@pytest.fixture()
def runner(app):
    return query.app.test_cli_runner()


def test_postgres(postgres_with_schema):
    """
    Check main postgres fixture
    """

    with postgres_with_schema.cursor() as cur:
      cur.execute("select * from dataselect_stats")
      cur.fetchall()


def test_wrong_parameter(client):
    """
    Check request with invalid parameter given
    """

    response = client.get('/statistics/1/query?wrong=stg')

    assert response.status_code == 400 and 'invalid parameter' in str(response.data)


def test_wrong_parameter_value(client):
    """
    Check request with invalid parameter given
    """

    response = client.get('/statistics/1/query?start=stg')

    assert response.status_code == 400 and 'invalid value of parameter' in str(response.data)


def test_correct_request(client):
    """
    Check request with invalid parameter given
    """

    response = client.get('/statistics/1/query?start=2021-05&country=GR&network=HL&aggregate_on=month,station,network,datacenter')

    assert response.status_code == 200
