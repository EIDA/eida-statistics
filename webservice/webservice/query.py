#!/usr/bin/env python3

import datetime
import model
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
    Returns status 400 Bad Request if not acceptable
    """

    app.logger.info('Checking parameters of request')

    accepted = ['start', 'end', 'data_center', 'network', 'station', 'country', 'location', 'channel']
    param_value_dict = {}
    for key in params:
        if key not in accepted:
            raise KeyError(key)
        else:
            # distinguish values given at each parameter
            # example of params.getlist(key): ["GR,FR", "SP"]
            temp = [p.split(",") for p in params.getlist(key)] # example of temp: [["GR", "FR"], "SP"]
            param_value_dict[key] = [x for y in temp for x in y] # example of param_value_dict[key]: ["GR", "FR", "SP"]
        if key == 'start':
            for p in param_value_dict[key]:
                try: date = datetime.datetime.strptime(params.get(key))
                except: raise ValueError(f"'{key}'")
                if not isinstance(date, "%Y-%m"):
                    raise ValueError(f"'{key}'")
        elif key == 'end':
            .
            .
            .

    return param_value_dict


@app.route('/statistics/1/dataselect', methods=['GET'])
def dataselect():
    """
    Returns statistics to be read by computer
    """

    app.logger.info('Entering dataselect')

    # check parameters and values
    # return bad request if not acceptable
    # otherwise return dictionary with parameters and values
    try:
        param_value_dict = check_request_parameters(request.args)

    except KeyError as e:
        return f"BAD REQUEST: invalid parameter " + str(e), 400

    except ValueError as e:
        return f"BAD REQUEST: invalid value of parameter " + str(e), 400

    #try:
    session = Session()
    response = session.query(model.DataselectStat).limit(3)
    session.close()

    #except:
        #app.logger.error('Database connection error or invalid SQL statement')

    # DON'T FORGET THE METADATA !!!
    return json.dumps([row.to_dict() for row in response], default=str)


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
        print('Database connection error')
