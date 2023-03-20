from pyramid.response import Response
from pyramid.view import view_config
from datetime import datetime
import os
import json
import mmh3
from ws_eidastats.helper_functions import log, Session
from sqlalchemy import exc
from sqlalchemy.sql import text


def get_node_from_token(token):
    """
    Returns the node name for a given token.
    Checks if the token is valid.
    Raise ValueError if the token provided is not in the database
    """
    log.debug("Token: %s", token)
    node = ""
    node_id = 0
    try:
        session = Session()
        sqlreq = session.execute(text("SELECT nodes.name, nodes.id from nodes join tokens on nodes.id = tokens.node_id where tokens.value=:tok and now() between tokens.valid_from and tokens.valid_until"), {'tok':token}).first()
        if sqlreq:
            node, node_id = sqlreq
        else:
            raise ValueError("No valid token found")
        session.close()

    except exc.DBAPIError as err:
        log.error("Postgresql error %s getting node from token", err.orig.pgcode)
        log.error(err.orig.pgerror)
        raise err
    log.info("Token is mapped to node %s", node)
    return node_id


def check_payload(payload):
    """
    Checks the payload format before trying to insert
    """
    check_stats = True
    check_metadata = 'generated_at' in payload.keys() and 'version' in payload.keys() and 'stats' in payload.keys() and 'days_coverage' in payload.keys()
    if check_metadata:
        for stat in payload['stats']:
            if 'month' in stat.keys() and 'clients' in stat.keys() and 'network' in stat.keys():
                continue
            check_stats = False
    return check_metadata and check_stats


def register_payload(node_id, payload):
    """
    Register payload to database
    """
    coverage = sorted([ datetime.strptime(v, '%Y-%m-%d') for v in payload['days_coverage'] ])
    log.debug(coverage)
    try:
        session = Session()
        # Insert bulk
        session.execute(text("INSERT INTO payloads (node_id, hash, version, generated_at, coverage)  VALUES (:n, :h, :v, :g, :c)"),
        {'n':node_id, 'h':mmh3.hash(str(payload['stats'])), 'v':payload['version'], 'g':payload['generated_at'], 'c':coverage})
        session.commit()
        session.close()
    except exc.DBAPIError as err:
        log.error("Postgresql error %s registering payload", err.orig.pgcode)
        log.error(err.orig.pgerror)
        if err.orig.pgcode == '23505':
            log.error("Duplicate payload")
            raise ValueError
        raise err


def register_statistics(statistics, node_id, operation='POST'):
    """
    Connects to the database and insert or update statistics
    params:
    - statistics is a list of dictionaries of all statistics, mapping to the table dataselect_stats schema but without the node_id
    - operation is the method POST of PUT
    """
    if operation == 'POST':
        sqlreq = text("""
                INSERT INTO dataselect_stats
                (
                  node_id, date, network, station, location, channel, country,
                  bytes, nb_reqs, nb_successful_reqs, nb_failed_reqs, clients
                )
                VALUES :values_list ON CONFLICT ON CONSTRAINT uniq_stat DO UPDATE SET
                bytes = EXCLUDED.bytes + dataselect_stats.bytes,
                nb_reqs = EXCLUDED.nb_reqs + dataselect_stats.nb_reqs,
                nb_successful_reqs = EXCLUDED.nb_successful_reqs + dataselect_stats.nb_successful_reqs,
                nb_failed_reqs = EXCLUDED.nb_failed_reqs + dataselect_stats.nb_failed_reqs,
                clients = EXCLUDED.clients || dataselect_stats.clients,
                updated_at = now()
                """)
    elif operation == 'PUT':
        sqlreq = text("""
                INSERT INTO dataselect_stats
                (
                  node_id, date, network, station, location, channel, country,
                  bytes, nb_reqs, nb_successful_reqs, nb_failed_reqs, clients
                )
                VALUES :values_list ON CONFLICT ON CONSTRAINT uniq_stat DO UPDATE SET
                bytes = EXCLUDED.bytes,
                nb_reqs = EXCLUDED.nb_reqs,
                nb_successful_reqs = EXCLUDED.nb_successful_reqs,
                nb_failed_reqs = EXCLUDED.nb_failed_reqs,
                clients = EXCLUDED.clients,
                created_at = now()
                """)
    else:
        log.error("Operation %s not supported (POST or PUT only)")
        raise ValueError

    # Add the nodeid to all elements of payload.
    # Convert list of dictionary to list of list
    values_list = []
    for item in statistics:
        log.debug("item: %s", item)
        # unify non-valid country codes as null value
        if len(item['country']) != 2:
            item['country'] = None;
        # if unsuccessful requests in Null, set it to 0
        if item['nb_unsuccessful_requests'] is None:
            item['nb_unsuccessful_requests'] = 0
        # if successful requests is Null, set it to nb_requests+nb_unsuccessful_requests
        if item['nb_requests'] is None:
            item['nb_requests'] = item['nb_successful_requests'] + item['nb_unsuccessful_requests']
        values_list.append((
            node_id, item['month'], item['network'], item['station'], item['location'], item['channel'], item['country'],
            item['bytes'], item['nb_requests'], item['nb_successful_requests'], item['nb_unsuccessful_requests'], item['clients']
        ))
    log.info(f"Registering {len(values_list)} statistics.")
    try:
        session = Session()
        # Insert bulk
        session.execute(sqlreq, [{'values_list':vl} for vl in values_list])
        session.commit()
        session.close()
    except exc.DBAPIError as err:
        log.error("Postgresql error %s registering statistic", err.orig.pgcode)
        log.error(err.orig.pgerror)
    log.info(f"Statistics successfully registered")

@view_config(route_name='submitstat')
def add_stat(request):
    """
    Adding the posted statistic to the database
    """
    log.info(f"{request.method} {request.url}")
    if request.method == 'GET':
        log.info(f"Method {request.method} not allowed")
        return Response(text="Only PUT or POST method allowed.", status_code=405, content_type='text/plain')

    log.info("Verifying token")

    # Check authentication token
    if request.headers.get('Authentication') is not None:
        log.debug("Headers: %s", request.headers.get('Authentication'))
        try:
            node_id = get_node_from_token(request.headers.get('Authentication').split(' ')[1])
        except ValueError:
            return Response(text="No valid token provided", status_code=403, content_type='text/plain')
        except exc.DBAPIError:
            return Response(text="Internal error", status_code=500, content_type='text/plain')
    else:
        return Response(text="No token provided. Permission denied", status_code=401, content_type='text/plain')

    log.info("Token verified. Analysing payload")
    # Analyse payload
    try:
        payload = request.json
        log.debug("Data is JSON")
    except:
        log.debug("Data is sent as other content type. Try to load as JSON")
        try:
            payload = json.loads(request.body)
        except Exception as err:
            log.error(request.body)
            log.error(err)
            return Response(text="Data can not be parsed as JSON format", status_code=400, content_type='text/plain')

    if not check_payload(payload):
        return Response(text="Malformed payload", status_code=400, content_type='text/plain')
    try:
        log.info("Registering statistics")
        register_payload(node_id, payload)
    except exc.DBAPIError:
        return Response(text="Internal error", status_code=500, content_type='text/plain')
    except ValueError:
        return Response(text="This statistic already exists on the server. Refusing to merge", status_code=400, content_type='text/plain')

    register_statistics(payload['stats'], node_id=node_id, operation=request.method)

    return Response(text="Statistic successfully ingested to database!", content_type='text/plain')
