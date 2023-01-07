#!/usr/bin/env python3

import datetime
import re
from model import Node, DataselectStat
import os
import logging
import psycopg2
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, text
from sqlalchemy.sql.expression import literal_column
import json
from flask import Flask, request

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# define database uri to connect to database
app.config['DBURI'] = os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')
engine = create_engine(app.config['DBURI'])
Session = sessionmaker(engine)


def check_request_parameters(request):
    """
    Checks if parameters and values given in a request are acceptable
    Returns dictionary with parameters and their values if acceptable
    Raises error if not acceptable
    """

    app.logger.debug('Entering check_request_parameters')

    accepted = ['start', 'end', 'datacenter', 'network', 'station', 'country', 'location', 'channel']
    # query method can take 2 more parameters
    if 'query' in request.url:
        accepted = accepted + ['aggregate_on', 'format']
    param_value_dict = {}
    params = request.args
    for key in params:
        if key not in accepted:
            raise KeyError(key)
        elif key in ['start', 'end']:
            # check date format, must be like 2021-05
            try: date = datetime.datetime.strptime(params.get(key), "%Y-%m")
            except: raise ValueError(f"'{key}'")
            # dates stored in database as every first day of a month
            param_value_dict[key] = params.get(key) + '-01'
        elif key == 'format':
            # format acceptable values: text or json
            if params.get(key) not in ['text', 'json']:
                raise ValueError(f"'{key}'")
            else:
                param_value_dict[key] = params.get(key)
        else:
            # distinguish values given at each parameter
            # example of params.getlist(key): ["GR,FR", "SP"] from http://some_url?country=GR,FR&otherparam=value&country=SP
            temp = [p.split(",") for p in params.getlist(key)] # example of temp: [["GR", "FR"], "SP"]
            param_value_dict[key] = [x for y in temp for x in y] # example of param_value_dict[key]: ["GR", "FR", "SP"]
            # wildcards handling
            if key in ['network', 'station', 'location', 'channel']:
                param_value_dict[key] = [s.replace('*', '%') for s in param_value_dict[key]]
                param_value_dict[key] = [s.replace('?', '_') for s in param_value_dict[key]]
            # aggregate_on parameter special handling
            if key == 'aggregate_on':
                if 'all' in param_value_dict[key]:
                    param_value_dict[key] = ['month', 'datacenter', 'network', 'station', 'country', 'location', 'channel']
                else:
                    for aggregator in param_value_dict[key]:
                        if aggregator not in ['month', 'datacenter', 'network', 'station', 'country', 'location', 'channel']:
                            raise ValueError(f"'{key}'")
                # default parameters to be aggregated in query method: location, channel
                if 'location' not in param_value_dict['aggregate_on']:
                    param_value_dict['aggregate_on'].append('location')
                if 'channel' not in param_value_dict['aggregate_on']:
                    param_value_dict['aggregate_on'].append('channel')

    # default parameters to be aggregated in query method: location, channel
    if 'query' in request.url and 'aggregate_on' not in param_value_dict:
        param_value_dict['aggregate_on'] = ['location', 'channel']
    # default output format: text
    if 'query' in request.url and 'format' not in param_value_dict:
        param_value_dict['format'] = 'text'

    return param_value_dict


@app.route('/statistics/1/dataselect', methods=['GET'])
def dataselect():
    """
    Returns statistics to be used by computer
    Returns bad request if invalid request parameter given
    """

    app.logger.debug('Entering dataselect')

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return bad request
    try:
        param_value_dict = check_request_parameters(request)

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
        # where clause
        if 'start' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date >= param_value_dict['start'])
        if 'end' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date <= param_value_dict['end'])
        if 'datacenter' in param_value_dict:
            sqlreq = sqlreq.filter(Node.name.in_(param_value_dict['datacenter']))
        if 'network' in param_value_dict:
            multiOR = or_(False)
            for net in param_value_dict['network']:
                if '%' or '_' in net:
                    multiOR = or_(multiOR, DataselectStat.network.like(net))
                else:
                    multiOR = or_(multiOR, DataselectStat.network == net)
            sqlreq = sqlreq.filter(multiOR)
        if 'station' in param_value_dict:
            multiOR = or_(False)
            for sta in param_value_dict['station']:
                if '%' or '_' in sta:
                    multiOR = or_(multiOR, DataselectStat.station.like(sta))
                else:
                    multiOR = or_(multiOR, DataselectStat.station == sta)
            sqlreq = sqlreq.filter(multiOR)
        if 'country' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.country.in_(param_value_dict['country']))
        if 'location' in param_value_dict:
            multiOR = or_(False)
            for loc in param_value_dict['location']:
                if '%' or '_' in loc:
                    multiOR = or_(multiOR, DataselectStat.location.like(loc))
                else:
                    multiOR = or_(multiOR, DataselectStat.location == loc)
            sqlreq = sqlreq.filter(multiOR)
        if 'channel' in param_value_dict:
            multiOR = or_(False)
            for cha in param_value_dict['channel']:
                if '%' or '_' in cha:
                    multiOR = or_(multiOR, DataselectStat.channel.like(cha))
                else:
                    multiOR = or_(multiOR, DataselectStat.channel == cha)
            sqlreq = sqlreq.filter(multiOR)
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


@app.route('/statistics/1/query', methods=['GET'])
def query():
    """
    Returns statistics to be read by human
    Returns bad request if invalid request parameter given
    """

    app.logger.debug('Entering query')

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return bad request
    try:
        param_value_dict = check_request_parameters(request)

    except KeyError as e:
        return f"BAD REQUEST: invalid parameter " + str(e), 400

    except ValueError as e:
        return f"BAD REQUEST: invalid value of parameter " + str(e), 400

    app.logger.info('Checked parameters of request')

    try:
        session = Session()
        sqlreq = session.query(DataselectStat).join(Node).with_entities()

        # if aggregate on a parameter don't select it
        # instead return '*' for it meaning all matching instances of parameter
        if 'month' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.date)
        if 'datacenter' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(Node.name)
        if 'network' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.network)
        if 'station' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.station)
        if 'country' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.country)
        if 'location' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.location)
        if 'channel' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.channel)

        # fields to be summed up
        sqlreq = sqlreq.add_columns(func.sum(DataselectStat.nb_reqs).label('nb_reqs'),func.sum(DataselectStat.nb_successful_reqs).label('nb_successful_reqs'),\
                    func.sum(DataselectStat.bytes).label('bytes'), literal_column('#hll_union_agg(dataselect_stats.clients)').label('clients'))

        # where clause
        if 'start' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date >= param_value_dict['start'])
        if 'end' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date <= param_value_dict['end'])
        if 'datacenter' in param_value_dict:
            sqlreq = sqlreq.filter(Node.name.in_(param_value_dict['datacenter']))
        if 'network' in param_value_dict:
            multiOR = or_(False)
            for net in param_value_dict['network']:
                if '%' or '_' in net:
                    multiOR = or_(multiOR, DataselectStat.network.like(net))
                else:
                    multiOR = or_(multiOR, DataselectStat.network == net)
            sqlreq = sqlreq.filter(multiOR)
        if 'station' in param_value_dict:
            multiOR = or_(False)
            for sta in param_value_dict['station']:
                if '%' or '_' in sta:
                    multiOR = or_(multiOR, DataselectStat.station.like(sta))
                else:
                    multiOR = or_(multiOR, DataselectStat.station == sta)
            sqlreq = sqlreq.filter(multiOR)
        if 'country' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.country.in_(param_value_dict['country']))
        if 'location' in param_value_dict:
            multiOR = or_(False)
            for loc in param_value_dict['location']:
                if '%' or '_' in loc:
                    multiOR = or_(multiOR, DataselectStat.location.like(loc))
                else:
                    multiOR = or_(multiOR, DataselectStat.location == loc)
            sqlreq = sqlreq.filter(multiOR)
        if 'channel' in param_value_dict:
            multiOR = or_(False)
            for cha in param_value_dict['channel']:
                if '%' or '_' in cha:
                    multiOR = or_(multiOR, DataselectStat.channel.like(cha))
                else:
                    multiOR = or_(multiOR, DataselectStat.channel == cha)
            sqlreq = sqlreq.filter(multiOR)

        # aggregate on requested parameters
        # group_by is the opposite process of the desired aggregation
        if 'month' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.date)
        if 'datacenter' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(Node.name)
        if 'network' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.network)
        if 'station' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.station)
        if 'country' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.country)
        if 'location' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.location)
        if 'channel' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.channel)
        session.close()

    except:
        return "Database connection error or invalid SQL statement passed to database", 500

    # get results as dictionaries
    # assign '*' at aggregated parameters
    results = []
    for row in sqlreq:

        rowToDict = DataselectStat.to_dict_for_query(row)
        rowToDict['month'] = str(row.date)[:-3] if 'month' not in param_value_dict['aggregate_on'] else '*'
        rowToDict['datacenter'] = row.name if 'datacenter' not in param_value_dict['aggregate_on'] else '*'
        rowToDict['network'] = row.network if 'network' not in param_value_dict['aggregate_on'] else '*'
        rowToDict['station'] = row.station if 'station' not in param_value_dict['aggregate_on'] else '*'
        rowToDict['country'] = row.country if 'country' not in param_value_dict['aggregate_on'] else '*'
        rowToDict['location'] = row.location if 'location' not in param_value_dict['aggregate_on'] else '*'
        rowToDict['channel'] = row.channel if 'channel' not in param_value_dict['aggregate_on'] else '*'
        results.append(rowToDict)

    # return json or text with metadata
    if param_value_dict['format'] == 'json':
        return json.dumps({'version': '1.0.0', 'matching': re.sub('&aggregate_on[^&]+', '', request.query_string.decode()),
                        'aggregated_on': ','.join(param_value_dict['aggregate_on']), 'results': results}, default=str)
    else:
        csvText = "# version: 1.0.0<br># matching: " + re.sub('&aggregate_on[^&]+', '', request.query_string.decode()) +\
            "<br># aggregated_on: " + ','.join(param_value_dict['aggregate_on']) +\
            "<br>month,datacenter,network,station,location,channel,country,bytes,nb_reqs,nb_successful_reqs,clients"
        for res in results:
            csvText += '<br>'
            for field in res:
                csvText += str(res[field]) + ','
            csvText = csvText[:-1]

        return csvText


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
