from pyramid.response import Response
from pyramid.view import view_config
from pyramid.view import notfound_view_config
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import json
import re
import mmh3
from webservice.model import Node, DataselectStat
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, text
from sqlalchemy.sql.expression import literal_column


log = logging.getLogger(__name__)


@notfound_view_config(append_slash=True)
def notfound_view(request):
    """
    Function invoked when user tries a non-existent route_name
    """

    log.info(f"{request.method} {request.url}")

    return Response("<h1>404 Not Found</h1>", status_code=404)


@view_config(route_name='health', request_method='GET', openapi=True)
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

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)


@view_config(route_name='nodes', request_method='GET', openapi=True)
def get_nodes(request):
    """
    Returns a list with the available datacenters
    """

    log.info(f"{request.method} {request.url}")

    try:
        with psycopg2.connect(request.registry.settings['DBURI']) as conn:
            with conn.cursor() as curs:
                curs.execute("select name from nodes")
                response = curs.fetchall()
                return Response(json={"nodes": [n for node in response for n in node]}, content_type='application/json')

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)


def check_request_parameters(request):
    """
    Checks if parameters and values given in a request are acceptable
    Returns dictionary with parameters and their values if acceptable
    Raises error if not acceptable
    """

    log.info('Entering check_request_parameters')

    accepted = ['start', 'end', 'datacenter', 'network', 'station', 'country', 'location', 'channel']
    # query method can take 2 more parameters
    if 'query' in request.url:
        accepted = accepted + ['aggregate_on', 'format']
    param_value_dict = {}
    params = request.params
    for key in params:
        if key not in accepted:
            raise KeyError(key)
        elif key in ['start', 'end']:
            # check date format, must be like 2021-05
            try: date = datetime.strptime(params.get(key), "%Y-%m")
            except: raise ValueError(key)
            # dates stored in database as every first day of a month
            param_value_dict[key] = params.get(key) + '-01'
        elif key == 'format':
            # format acceptable values: csv or json
            if params.get(key) not in ['csv', 'json']:
                raise ValueError(key)
            else:
                param_value_dict[key] = params.get(key)
        else:
            # distinguish values given at each parameter
            # example of params.getall(key): ["GR,FR", "SP"] from http://some_url?country=GR,FR&otherparam=value&country=SP
            temp = [p.split(",") for p in params.getall(key)] # example of temp: [["GR", "FR"], "SP"]
            param_value_dict[key] = [x for y in temp for x in y] # example of param_value_dict[key]: ["GR", "FR", "SP"]
            # wildcards handling
            if key in ['network', 'station', 'location', 'channel']:
                param_value_dict[key] = [s.replace('*', '%') for s in param_value_dict[key]]
                param_value_dict[key] = [s.replace('?', '_') for s in param_value_dict[key]]
            elif key == 'datacenter':
                acceptable_nodes = get_nodes(request).json['nodes']
                if any(x not in acceptable_nodes for x in param_value_dict['datacenter']):
                    raise ValueError(key)
            # aggregate_on parameter special handling
            elif key == 'aggregate_on':
                if 'all' in param_value_dict[key]:
                    param_value_dict[key] = ['month', 'datacenter', 'network', 'station', 'country', 'location', 'channel']
                else:
                    for aggregator in param_value_dict[key]:
                        if aggregator not in ['month', 'datacenter', 'network', 'station', 'country', 'location', 'channel']:
                            raise ValueError(key)
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


@view_config(route_name='dataselectstats', request_method='GET', openapi=True)
def dataselectstats(request):
    """
    Returns statistics to be used by computer
    Returns bad request if invalid request parameter given
    """

    log.info(f"{request.method} {request.url}")

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return bad request
    try:
        param_value_dict = check_request_parameters(request)

    except KeyError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter {str(e)}</p>", status_code=400)

    except ValueError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Unsupported value for parameter '{str(e)}'</p>", status_code=400)

    except LookupError:
        return Response("<h1>400 Bad Request</h1><p>Specify at least one of 'start' or 'end' parameters</p>", status_code=400)

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    try:
        engine = create_engine(request.registry.settings['DBURI'])
        Session = sessionmaker(engine)

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

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error or invalid SQL statement passed to database</p>", status_code=500)

    # get results as dictionaries and add datacenter name
    results = []
    for row in sqlreq:
        rowToDict = DataselectStat.to_dict(row)
        rowToDict['datacenter'] = row.name
        results.append(rowToDict)

    # return json with metadata
    return Response(text=json.dumps({'version': '1.0.0', 'request_parameters': request.query_string, 'results': results},
            default=str), content_type='application/json', charset='utf-8')


@view_config(route_name='dataselectquery', request_method='GET', openapi=True)
def query(request):
    """
    Returns statistics to be read by human
    Returns bad request if invalid request parameter given
    """

    log.info(f"{request.method} {request.url}")

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return bad request
    try:
        param_value_dict = check_request_parameters(request)

    except KeyError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter {str(e)}</p>", status_code=400)

    except ValueError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Unsupported value for parameter '{str(e)}'</p>", status_code=400)

    except LookupError:
        return Response("<h1>400 Bad Request</h1><p>Specify at least one of 'start' or 'end' parameters</p>", status_code=400)

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    try:
        engine = create_engine(request.registry.settings['DBURI'])
        Session = sessionmaker(engine)

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
        sqlreq = sqlreq.add_columns(func.sum(DataselectStat.nb_reqs).label('nb_reqs'),
                    func.sum(DataselectStat.nb_successful_reqs).label('nb_successful_reqs'),
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

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error or invalid SQL statement passed to database</p>", status_code=500)

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
        return Response(text=json.dumps({'version': '1.0.0', 'matching': re.sub('&aggregate_on[^&]+', '', request.query_string),
                'aggregated_on': ','.join(param_value_dict['aggregate_on']), 'results': results}, default=str),
                content_type='application/json', charset='utf-8')
    else:
        csvText = "# version: 1.0.0\n# matching: " + re.sub('&aggregate_on[^&]+', '', request.query_string) +\
            "\n# aggregated_on: " + ','.join(param_value_dict['aggregate_on']) +\
            "\nmonth,datacenter,network,station,location,channel,country,bytes,nb_reqs,nb_successful_reqs,clients"
        for res in results:
            csvText += '\n'
            for field in res:
                csvText += str(res[field]) + ','
            csvText = csvText[:-1]

        return Response(text=csvText, content_type='text/csv')


def get_node_from_token(token, request):
    """
    Returns the node name for a given token.
    Checks if the token is valid.
    Raise ValueError if the token provided is not in the database
    """
    log.debug("Token: %s", token)
    node = ""
    node_id = 0
    try:
        with psycopg2.connect(request.registry.settings['DBURI']) as conn:
            with conn.cursor() as curs:
                curs.execute("SELECT nodes.name, nodes.id from nodes join tokens on nodes.id = tokens.node_id where tokens.value=%s and now() between tokens.valid_from and tokens.valid_until", (token,))
                if curs.rowcount == 1:
                    node, node_id = curs.fetchone()
                else:
                    raise ValueError("No valid token found")
    except psycopg2.Error as err:
        log.error("Postgresql error %s getting node from token", err.pgcode)
        log.error(err.pgerror)
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


def register_payload(node_id, payload, request):
    """
    Register payload to database
    """
    coverage = sorted([ datetime.strptime(v, '%Y-%m-%d') for v in payload['days_coverage'] ])
    log.debug(coverage)
    try:
        with psycopg2.connect(request.registry.settings['DBURI']) as conn:
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
        log.error("Postgresql error %s registering payload", err.pgcode)
        log.error(err.pgerror)
        if err.pgcode == '23505':
            log.error("Duplicate payload")
            raise ValueError
        raise err


def register_statistics(statistics, node_id, request, operation='POST'):
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
        values_list.append( [
            node_id, item['month'], item['network'], item['station'], item['location'], item['channel'], item['country'],
            item['bytes'], item['nb_requests'], item['nb_successful_requests'], item['nb_unsuccessful_requests'], item['clients']
        ])
    try:
        with psycopg2.connect(request.registry.settings['DBURI']) as conn:
            with conn.cursor() as curs:
                # Insert bulk
                execute_values(curs, sqlreq, values_list)
    except psycopg2.Error as err:
        log.error("Postgresql error %s registering statistic", err.pgcode)
        log.error(err.pgerror)


@view_config(route_name='submitstat', request_method=['POST', 'PUT'])
def add_stat(request):
    """
    Adding the posted statistic to the database
    """
    log.info(f"{request.method} {request.url}")
    log.info("Receiving statistics")

    # Check authentication token
    if request.headers.get('Authentication') is not None:
        log.debug("Headers: %s", request.headers.get('Authentication'))
        try:
            node_id = get_node_from_token(request.headers.get('Authentication').split(' ')[1], request)
        except ValueError:
            return Response(text="No valid token provided", status_code=403, content_type='text/plain')
        except psycopg2.Error:
            return Response(text="Internal error", status_code=500, content_type='text/plain')
    else:
        return Response(text="No token provided. Permission denied", status_code=401, content_type='text/plain')

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
        register_payload(node_id, payload, request)
    except psycopg2.Error:
        return Response(text="Internal error", status_code=500, content_type='text/plain')
    except ValueError:
        return Response(text="This statistic already exists on the server. Refusing to merge", status_code=400, content_type='text/plain')

    register_statistics(payload['stats'], node_id=node_id, request=request, operation=request.method)

    return Response(text="Statistic successfully ingested to database!", content_type='text/plain')
