from pyramid.response import Response
from pyramid.view import view_config
import psycopg2
from datetime import datetime
import logging


log = logging.getLogger(__name__)


@view_config(route_name='hello')
def hello_world(request):
    return Response(json={'message': 'Hello World!'})


@view_config(route_name='health', request_method='GET')
def test_database(request):
    """
    Returns a 200 OK message if the webservice is running and database is available
    """

    log.info(f"{request.method} {request.url}")

    try:
        with psycopg2.connect(request.registry.settings['DBURI']) as conn:
            with conn.cursor() as curs:
                curs.execute("select * from dataselect_stats limit 3")
                response = curs.fetchall()
                return Response(text="The service is up and running and database is available!", content_type='text/plain')

    except:
        return Response(text="Database connection error", status_code=500, content_type='text/plain')
