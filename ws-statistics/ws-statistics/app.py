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
                curs.execute("SELECT nodes.id, nodes.name from nodes join tokens on nodes.id = tokens.node_id where tokens.value=%s and now() between tokens.valid_from and tokens.valid_until", (token,))
                if curs.rowcount == 1:
                    node, node_id = curs.fetchone()
                else:
                    raise ValueError("No valid token found")
    except psycopg2.Error as err:
        logger.error("Postgresql error %s registering statistic", err)
        logger.error(err.pgerror)
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
                INSERT INTO dataselect_stats VALUES %s ON CONSTRAINT uniq_stat DO UPDATE SET
                bytes = EXCLUDED.bytes + dataselect_stats.bytes,
                nb_reqs = EXCLUDED.nb_reqs + dataselect_stats.nb_reqs,
                nb_successful_reqs = EXCLUDED.nb_successful_reqs + dataselect_stats.nb_successful_reqs,
                nb_failedrequests = EXCLUDED.nb_failedrequests + dataselect_stats.nb_failedrequests,
                -- WRONG : TODO clients is an HLL object and it's more complicated
                clients = EXCLUDED.clients + dataselect_stats.clients,
                updated_at = EXCLUDED.updated_at
                """
    elif operation == 'PUT':
        sqlreq = """
                INSERT INTO dataselect_stats VALUES %s ON CONSTRAINT uniq_stat DO UPDATE SET
                bytes = EXCLUDED.bytes
                nb_reqs = EXCLUDED.nb_reqs
                nb_successful_reqs = EXCLUDED.nb_successful_reqs,
                nb_failedrequests = EXCLUDED.nb_failedrequests,
                clients = EXCLUDED.clients,
                created_at = now()
                """
    else:
        logger.error("Operation %s not supported (POST or PUT only)")
        raise ValueError

    # Add the nodeid to all elements of payload.
    for item in statistics:
        item.update( {"node_id":"node_id", "updated_at": datetime.now()})
    try:
        with psycopg2.connect(app.config['DBURI']) as conn:
            with conn.cursor() as curs:
                # Insert bulk
                execute_values(curs, sqlreq, statistics)
    except psycopg2.Error as err:
        logger.error("Postgresql error %s registering statistic", err.pgcode)
        logger.errir(err.pgerror)

@app.route('/dataselectstats')
def welcome():
    return "Welcome to dataselect statistics. Please POST your statistics."


@app.route('/dataselectstats',methods=['POST', 'PUT'])
def add_stat():
    """
    Adding the posted statistic to the database
    """
    payload = request.files
    app.logger.debug("Got payload: %s", payload)
    app.logger.debug("Headers: %s", request.headers.get('Authentication'))

    try:
        node_id = get_node_from_token(request.headers.get('Authentication').lstrip("Bearer "))
    except ValueError:
        return ("No valid token provided", 403)
    check_payload(payload)
    register_statistics(payload, node_id=node_id, operation=request.method)

    return "OK"
