#!/usr/bin/env python3

import os
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import execute_values
import mmh3
import json
from flask import Flask, request

app = Flask(__name__)
app.config['DBURI']=os.getenv('DBURI', 'postgresql://postgres@localhost:5432/eidastats')

def get_node_from_token(token):
    """
    Returns the node name for a given token.
    Checks if the token is valid.
    Raise ValueError if the token provided is not in the database
    """
    app.logger.debug("Token: %s", token)
    node = ""
    node_id = 0
    try:
        with psycopg2.connect(app.config['DBURI']) as conn:
            with conn.cursor() as curs:
                curs.execute("SELECT nodes.name, nodes.id from nodes join tokens on nodes.id = tokens.node_id where tokens.value=%s and now() between tokens.valid_from and tokens.valid_until", (token,))
                if curs.rowcount == 1:
                    node, node_id = curs.fetchone()
                else:
                    raise ValueError("No valid token found")
    except psycopg2.Error as err:
        app.logger.error("Postgresql error %s getting node from token", err.pgcode)
        app.logger.error(err.pgerror)
        raise err
    app.logger.info("Token is mapped to node %s", node)
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
    app.logger.debug(coverage)
    try:
        with psycopg2.connect(app.config['DBURI']) as conn:
            with conn.cursor() as curs:
                # Insert bulk
                curs.execute("""
                INSERT INTO payloads (node_id, hash, version, generated_at, coverage)  VALUES
                (%s, %s, %s, %s, %s )
                """,
                             (node_id,
                              mmh3.hash(str(payload['stats'])),
                              payload['version'],
                              payload['generated_at'],
                              coverage))
    except psycopg2.Error as err:
        app.logger.error("Postgresql error %s registering payload", err.pgcode)
        app.logger.error(err.pgerror)
        if err.pgcode == '23505':
            app.logger.error("Duplicate payload")
            raise ValueError
        raise err


def register_statistics(statistics, node_id, operation='POST'):
    """
    Connects to the database and insert or update statistics
    params:
    - statistics is a list of dictionaries of all statistics, mapping to the table dataselect_stats schema but without the node_id
    - opetation is the method POST of PUT
    """
    if operation == 'POST':
        sqlreq = """
                INSERT INTO dataselect_stats
                (
                  node_id, date, network, station, location, channel, country,
                  bytes, nb_reqs, nb_successful_reqs, nb_failed_reqs, clients
                )
                VALUES %s ON CONFLICT ON CONSTRAINT uniq_stat DO UPDATE SET
                bytes = EXCLUDED.bytes + dataselect_stats.bytes,
                nb_reqs = EXCLUDED.nb_reqs + dataselect_stats.nb_reqs,
                nb_successful_reqs = EXCLUDED.nb_successful_reqs + dataselect_stats.nb_successful_reqs,
                nb_failed_reqs = EXCLUDED.nb_failed_reqs + dataselect_stats.nb_failed_reqs,
                clients = EXCLUDED.clients || dataselect_stats.clients,
                updated_at = now()
                """
    elif operation == 'PUT':
        sqlreq = """
                INSERT INTO dataselect_stats
                (
                  node_id, date, network, station, location, channel, country,
                  bytes, nb_reqs, nb_successful_reqs, nb_failed_reqs, clients
                )
                VALUES %s ON CONFLICT ON CONSTRAINT uniq_stat DO UPDATE SET
                bytes = EXCLUDED.bytes,
                nb_reqs = EXCLUDED.nb_reqs,
                nb_successful_reqs = EXCLUDED.nb_successful_reqs,
                nb_failed_reqs = EXCLUDED.nb_failed_reqs,
                clients = EXCLUDED.clients,
                created_at = now()
                """
    else:
        app.logger.error("Operation %s not supported (POST or PUT only)")
        raise ValueError

    # Add the nodeid to all elements of payload.
    # Convert list of dictionary to list of list
    values_list = []
    for item in statistics:
        app.logger.debug("item: %s", item)
        values_list.append( [
            node_id, item['month'], item['network'], item['station'], item['location'], item['channel'], item['country'],
            item['bytes'], item['nb_requests'], item['nb_successful_requests'], item['nb_unsuccessful_requests'], item['clients']
        ])
    try:
        with psycopg2.connect(app.config['DBURI']) as conn:
            with conn.cursor() as curs:
                # Insert bulk
                execute_values(curs, sqlreq, values_list)
    except psycopg2.Error as err:
        app.logger.error("Postgresql error %s registering statistic", err.pgcode)
        app.logger.error(err.pgerror)

@app.route('/dataselectstats')
def welcome():
    return "Welcome to dataselect statistics. Please POST your statistics."


@app.route('/dataselectstats',methods=['POST', 'PUT'])
def add_stat():
    """
    Adding the posted statistic to the database
    """
    app.logger.info("Receiving statistics")
    if request.is_json:
        app.logger.debug("Data is JSON")
        payload = request.get_json()
    else:
        app.logger.debug("Data is sent as other content type. Try to load as JSON")
        try:
            payload = json.loads(request.data)
        except Exception as err:
            app.logger.error(request.data)
            app.logger.error(err)
            return("Data can not be parsed as JSON format", 400)

    app.logger.debug("Headers: %s", request.headers.get('Authentication'))

    if request.headers.get('Authentication') is not None:
        try:
            node_id = get_node_from_token(request.headers.get('Authentication').split(' ')[1])
        except ValueError:
            return ("No valid token provided", 403)
        except psycopg2.Error:
            return ("Internal error", 500)
    else:
        return ("No token provided. Permission denied", 401)
    if not check_payload(payload):
        return("Malformed payload", 400)
    try:
        app.logger.info("Registering statistics")
        register_payload(node_id, payload)
    except psycopg2.Error:
        return ("Internal error", 500)
    except ValueError:
        return ("This statistic already exists on the server. Refusing to merge", 400)

    register_statistics(payload['stats'], node_id=node_id, operation=request.method)

    return "OK"
