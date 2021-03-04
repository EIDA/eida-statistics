#!/usr/bin/env python3

import os
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import execute_values
from flask import Flask, request

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
app = Flask(__name__)
app.config['DBURI']=os.getenv('DBURI', 'postgresql://wseidastats@localhost:5432/eidastatistics')

def get_node_from_token(token):
    """
    Returns the node name for a given token.
    Checks if the token is valid.
    Raise ValueError if the token provided is not in the database
    """
    logger.debug("Token: %s", token)
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
        logger.error("Postgresql error %s registering statistic", err)
        logger.error(err.pgerror)
        raise err
    logger.info("Token is mapped to node %s", node)
    return node_id

def check_payload(payload):
    """
    Checks the payload format before trying to insert
    TODO
    """

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
        logger.error("Operation %s not supported (POST or PUT only)")
        raise ValueError

    # Add the nodeid to all elements of payload.
    # Convert list of dictionary to list of list
    values_list = []
    for item in statistics:
        values_list.append( [
            node_id, item['date'], item['network'], item['station'], item['location'], item['channel'], item['country'],
            item['bytes'], item['nb_requests'], item['nb_successful_requests'], item['nb_unsuccessful_requests'], item['clients']
        ])
    try:
        with psycopg2.connect(app.config['DBURI']) as conn:
            with conn.cursor() as curs:
                # Insert bulk
                execute_values(curs, sqlreq, values_list)
    except psycopg2.Error as err:
        logger.error("Postgresql error %s registering statistic", err.pgcode)
        logger.error(err.pgerror)

@app.route('/dataselectstats')
def welcome():
    return "Welcome to dataselect statistics. Please POST your statistics."


@app.route('/dataselectstats',methods=['POST', 'PUT'])
def add_stat():
    """
    Adding the posted statistic to the database
    """
    payload = request.json
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
    check_payload(payload)
    register_statistics(payload, node_id=node_id, operation=request.method)

    return "OK"
