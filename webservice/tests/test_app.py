#!/usr/bin/env python3

import pytest
from pytest_postgresql import factories
from webservice import app


postgresql_my_proc = factories.postgresql_proc(
    port=None, unixsocketdir='/var/run')
postgresql_my_with_schema = factories.postgresql('postgresql_my_proc', load=['eidastats_schema.sql'])


