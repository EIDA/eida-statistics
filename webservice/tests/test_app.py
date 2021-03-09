#!/usr/bin/env python3

import pytest
from pytest_postgresql import factories
from webservice import app


postgresql_my_proc = factories.postgresql_noproc(host="localhost", port="5432", password="password")
postgres_with_schema = factories.postgresql('postgresql_my_proc', db_name="test", load=['tests/eidastats_schema.sql'])

def test_postgres_1(postgres_with_schema):
    """
    Check main postgres fixture
    """
    with postgres_with_schema.cursor() as cur:
      cur.execute("select * from tokens")
      print(cur.fetchall())
