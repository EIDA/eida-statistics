#!/usr/bin/env python3

from datetime import datetime
import re
from model import Node, DataselectStat
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
import mmh3
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, text
from sqlalchemy.sql.expression import literal_column
import json
from flask import Flask, request, render_template
from flask_swagger_ui import get_swaggerui_blueprint


app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# define database URI to connect to database
app.config['DBURI'] = os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')
engine = create_engine(app.config['DBURI'])
Session = sessionmaker(engine)

# define server URL
app.config['EIDASTATS_API_HOST'] = os.getenv('EIDASTATS_API_HOST', 'localhost:5000')
app.config['EIDASTATS_API_PATH'] = os.getenv('EIDASTATS_API_PATH', '')

# documentation page
swaggerui_blueprint = get_swaggerui_blueprint(app.config['EIDASTATS_API_PATH'], '/static/openapi.yaml',
    config={'app_name': "Statistics Webservice Documentation", 'layout': "BaseLayout"})
app.register_blueprint(swaggerui_blueprint)


@app.route('/dataselect/old_doc')
def documentation():
    """
    Shows the documentation page of the statistics webservice
    """

    return render_template('doc.html',
        url_dataselect='http://'+app.config['EIDASTATS_API_HOST']+app.config['EIDASTATS_API_PATH']+'/dataselect/stats/builder',
        url_query='http://'+app.config['EIDASTATS_API_HOST']+app.config['EIDASTATS_API_PATH']+'/dataselect/query/builder')


@app.route('/dataselect/stats/builder')
def dataselectstats_builder():
    """
    Builder and documentation page for the dataselectstats method
    """

    return render_template('dataselectstats.html',
        url_dataselect='http://'+app.config['EIDASTATS_API_HOST']+app.config['EIDASTATS_API_PATH']+'/dataselect/stats',
        url_nodes='http://'+app.config['EIDASTATS_API_HOST']+app.config['EIDASTATS_API_PATH']+'/_nodes')


@app.route('/dataselect/query/builder')
def query_builder():
    """
    Builder and documentation page for the query method
    """

    return render_template('query.html',
        url_query='http://'+app.config['EIDASTATS_API_HOST']+app.config['EIDASTATS_API_PATH']+'/dataselect/query',
        url_nodes='http://'+app.config['EIDASTATS_API_HOST']+app.config['EIDASTATS_API_PATH']+'/_nodes')


@app.route('/_health')
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


@app.route('/_nodes')
def get_nodes():
    """
    Returns a list with the available datacenters
    """

    try:
        with psycopg2.connect(app.config['DBURI']) as conn:
            with conn.cursor() as curs:
                curs.execute("select name from nodes")
                response = curs.fetchall()
                return json.dumps({"nodes": [n for node in response for n in node]}), {'Content-Type': 'application/json'}
    except:
        return "Database connection error", 500


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
            try: date = datetime.strptime(params.get(key), "%Y-%m")
            except: raise ValueError(f"'{key}'")
            # dates stored in database as every first day of a month
            param_value_dict[key] = params.get(key) + '-01'
        elif key == 'format':
            # format acceptable values: csv or json
            if params.get(key) not in ['csv', 'json']:
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

    # make some parameters mandatory
    if 'start' not in param_value_dict and 'end' not in param_value_dict:
        raise LookupError
    # default parameters to be aggregated in query method: location, channel
    if 'query' in request.url and 'aggregate_on' not in param_value_dict:
        param_value_dict['aggregate_on'] = ['location', 'channel']
    # default output format: csv
    if 'query' in request.url and 'format' not in param_value_dict:
        param_value_dict['format'] = 'csv'

    return param_value_dict


@app.route('/dataselect/stats', methods=['GET'])
def dataselectstats():
    """
    Returns statistics to be used by computer
    Returns bad request if invalid request parameter given
    """

    app.logger.debug('Entering dataselectstats')

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return bad request
    try:
        param_value_dict = check_request_parameters(request)

    except KeyError as e:
        return f"BAD REQUEST: invalid parameter " + str(e), 400, {'Content-Type': 'text/plain'}

    except ValueError as e:
        return f"BAD REQUEST: invalid value of parameter " + str(e), 400, {'Content-Type': 'text/plain'}

    except LookupError:
        return "BAD REQUEST: define at least one of 'start' or 'end' parameters", 400, {'Content-Type': 'text/plain'}

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
        return "Database connection error or invalid SQL statement passed to database", 500, {'Content-Type': 'text/plain'}

    # get results as dictionaries and add datacenter name
    results = []
    for row in sqlreq:
        rowToDict = DataselectStat.to_dict(row)
        rowToDict['datacenter'] = row.name
        results.append(rowToDict)

    # return json with metadata
    return json.dumps({'version': '1.0.0', 'request_parameters': request.query_string.decode(),
                    'results': results}, default=str), {'Content-Type': 'application/json'}


@app.route('/dataselect/query', methods=['GET'])
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
        return f"BAD REQUEST: invalid parameter " + str(e), 400, {'Content-Type': 'text/plain'}

    except ValueError as e:
        return f"BAD REQUEST: invalid value of parameter " + str(e), 400, {'Content-Type': 'text/plain'}

    except LookupError:
        return "BAD REQUEST: define at least one of 'start' or 'end' parameters", 400, {'Content-Type': 'text/plain'}

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
        return "Database connection error or invalid SQL statement passed to database", 500, {'Content-Type': 'application/json'}

    # get results as dictionaries
    # assign '*' at aggregated parameters
    results = []
    for row in sqlreq:

        if row != (None, None, None, None):
            rowToDict = DataselectStat.to_dict_for_query(row)
            rowToDict['month'] = str(row.date)[:-3] if 'month' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['datacenter'] = row.name if 'datacenter' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['network'] = row.network if 'network' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['station'] = row.station if 'station' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['country'] = row.country if 'country' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['location'] = row.location if 'location' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['channel'] = row.channel if 'channel' not in param_value_dict['aggregate_on'] else '*'
            results.append(rowToDict)

    # return json or csv with metadata
    if param_value_dict['format'] == 'json':
        return json.dumps({'version': '1.0.0', 'matching': re.sub('&aggregate_on[^&]+', '', request.query_string.decode()),
                        'aggregated_on': ','.join(param_value_dict['aggregate_on']), 'results': results}, default=str), {'Content-Type': 'application/json'}
    else:
        csvText = "# version: 1.0.0\n# matching: " + re.sub('&aggregate_on[^&]+', '', request.query_string.decode()) +\
            "\n# aggregated_on: " + ','.join(param_value_dict['aggregate_on']) +\
            "\nmonth,datacenter,network,station,location,channel,country,bytes,nb_reqs,nb_successful_reqs,clients"
        for res in results:
            csvText += '\n'
            for field in res:
                csvText += str(res[field]) + ','
            csvText = csvText[:-1]

        return csvText, {'Content-Type': 'text/csv'}


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


@app.route('/dataselect',methods=['POST', 'PUT'])
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
