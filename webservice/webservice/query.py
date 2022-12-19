#!/usr/bin/env python3

import datetime
from model import Node, DataselectStat
import os
import logging
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
import json
from flask import Flask, request

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# define database uri to connect to database
app.config['DBURI'] = os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')
engine = create_engine(app.config['DBURI'])
Session = sessionmaker(engine)


def check_request_parameters(params):
    """
    Checks if parameters and values given in a request are acceptable
    Returns dictionary with parameters and their values if acceptable
    Raises error if not acceptable
    """

    app.logger.debug('Entering check_request_parameters')

    accepted = ['start', 'end', 'datacenter', 'network', 'station', 'country', 'location', 'channel']
    param_value_dict = {}
    for key in params:
        if key not in accepted:
            raise KeyError(key)
        elif key in ['start', 'end']:
            # check date format, must be like 2021-05
            try: date = datetime.datetime.strptime(params.get(key), "%Y-%m")
            except: raise ValueError(f"'{key}'")
            # dates stored in database as every first day of a month
            param_value_dict[key] = params.get(key) + '-01'
        else:
            # distinguish values given at each parameter
            # example of params.getlist(key): ["GR,FR", "SP"] from http://some_url?country=GR,FR&otherparam=value&country=SP
            temp = [p.split(",") for p in params.getlist(key)] # example of temp: [["GR", "FR"], "SP"]
            param_value_dict[key] = [x for y in temp for x in y] # example of param_value_dict[key]: ["GR", "FR", "SP"]

    return param_value_dict


@app.route('/statistics/1/dataselect', methods=['GET'])
def dataselect():
    """
    Returns statistics to be read by computer
    """

    app.logger.debug('Entering dataselect')

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return bad request
    try:
        param_value_dict = check_request_parameters(request.args)

    except KeyError as e:
        return f"BAD REQUEST: invalid parameter " + str(e), 400

    except ValueError as e:
        return f"BAD REQUEST: invalid value of parameter " + str(e), 400

    app.logger.info('Checked parameters of request')

    try:
        session = Session()
        sqlreq = session.query(DataselectStat).join(Node).\
                            with_entities(DataselectStat.date, DataselectStat.network, DataselectStat.station, DataselectStat.location,\
                            DataselectStat.channel, DataselectStat.country, DataselectStat.nb_reqs, DataselectStat.nb_successful_reqs,\
                            DataselectStat.bytes, DataselectStat.clients, Node.name)
        if 'start' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date >= param_value_dict['start'])
        if 'end' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date <= param_value_dict['end'])
        if 'network' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.network.in_(param_value_dict['network']))
        if 'station' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.station.in_(param_value_dict['station']))
        if 'country' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.country.in_(param_value_dict['country']))
        if 'location' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.location.in_(param_value_dict['location']))
        if 'channel' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.channel.in_(param_value_dict['channel']))
        if 'datacenter' in param_value_dict:
            sqlreq = sqlreq.filter(Node.name.in_(param_value_dict['datacenter']))
        session.close()

    except:
        return "Database connection error or invalid SQL statement passed to database", 500

    # get results as dictionaries and add datacenter name
    results = []
    for row in sqlreq:
        rowToDict = DataselectStat.to_dict(row)
        rowToDict['datacenter'] = row.name
        results.append(rowToDict)

    # return json with metadata
    return json.dumps({'version': '1.0.0', 'request_parameters': request.query_string.decode(),
                    'results': results}, default=str)


@app.route('/statistics/1/health')
def test_database():
    """
    Returns a 200 OK message if the webservice is running and database is available
    """

    try:
        with psycopg2.connect(app.config['DBURI']) as conn:
            with conn.cursor() as curs:
                curs.execute("select * from dataselect_stats limit 3")
                response = curs.fetchall()
                return "The service is up and running and database is available!", 200

    except:
        return "Database connection error", 500
