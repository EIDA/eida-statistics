from pyramid.response import Response
from pyramid.view import view_config
from pyramid.view import notfound_view_config
from datetime import datetime
import os
import logging
import json
import re
import mmh3
import requests
import gnupg
from ws_eidastats.model import Node, DataselectStat, Network
from sqlalchemy import create_engine, or_, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, text
from sqlalchemy.sql.expression import literal_column


log = logging.getLogger(__name__)
dbURI = os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')
engine = create_engine(dbURI)
Session = sessionmaker(engine)


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
        session = Session()
        sqlreq = session.query(DataselectStat).limit(3).all()
        session.close()
        return Response(text="The service is up and running and database is available!", content_type='text/plain')

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)


@view_config(route_name='nodes', request_method='GET', openapi=True)
def get_nodes(request, internalCall=False):
    """
    Returns a list with the available datacenters
    """

    if internalCall:
        log.info('Entering get_nodes')
    else:
        log.info(f"{request.method} {request.url}")

    try:
        session = Session()
        sqlreq = session.query(Node).with_entities(Node.name).all()
        session.close()
        return Response(json={"nodes": [n for node in sqlreq for n in node]}, content_type='application/json')

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

    # parameters that all methods accept
    accepted = ['start', 'end', 'datacenter', 'country']
    # parameters accepted by raw method
    if 'raw' in request.url:
        accepted += ['network', 'station', 'location', 'channel']
    # parameters accepted by restricted method
    if 'restricted' in request.url:
        accepted += ['network', 'station', 'location', 'channel', 'aggregate_on', 'format']
    # parameters accepted by public method
    if 'public' in request.url:
        accepted += ['aggregate_on', 'format']

    param_value_dict = {}
    params = request.params
    for key in params:
        log.debug('Parameter: '+key)
        if key not in accepted:
            raise KeyError(key)
        elif key in ['start', 'end']:
            # check date format, must be like 2021-05
            try:
                log.debug('Date: '+params.get(key))
                date = datetime.strptime(params.get(key), "%Y-%m")
            except:
                raise ValueError(key)
            # dates stored in database as every first day of a month
            param_value_dict[key] = params.get(key) + '-01'
        elif key == 'format':
            # format acceptable values: csv or json
            log.debug('Format: '+params.get(key))
            if params.get(key) not in ['csv', 'json']:
                raise ValueError(key)
            else:
                param_value_dict[key] = params.get(key)
        else:
            # distinguish values given at each parameter
            # example of params.getall(key): ["GR,FR", "SP"] from http://some_url?country=GR,FR&otherparam=value&country=SP
            log.debug(params.getall(key))
            temp = [p.split(",") for p in params.getall(key)] # example of temp: [["GR", "FR"], "SP"]
            param_value_dict[key] = [x for y in temp for x in y] # example of param_value_dict[key]: ["GR", "FR", "SP"]
            log.debug('Multivalue fixed: '+str(param_value_dict[key]))
            # wildcards handling
            if key in ['network', 'station', 'location', 'channel']:
                param_value_dict[key] = [s.replace('*', '%') for s in param_value_dict[key]]
                param_value_dict[key] = [s.replace('?', '_') for s in param_value_dict[key]]
                log.debug('After wildcards: '+str(param_value_dict[key]))
            elif key == 'datacenter':
                try:
                    acceptable_nodes = get_nodes(request, internalCall=True).json['nodes']
                except:
                    raise Exception
                log.info('Got available datacenters from database')
                if any(x not in acceptable_nodes for x in param_value_dict[key]):
                    raise ValueError(key)
            # aggregate_on parameter special handling
            elif key == 'aggregate_on':
                if 'all' in param_value_dict[key]:
                    param_value_dict[key] = ['month', 'datacenter', 'network', 'station', 'country', 'location', 'channel']
                else:
                    if any(x not in ['month', 'datacenter', 'network', 'station', 'country', 'location', 'channel'] for x in param_value_dict[key]):
                        raise ValueError(key)
                # default parameters to be aggregated in restricted and public methods: location, channel
                if 'location' not in param_value_dict[key]:
                    param_value_dict[key].append('location')
                if 'channel' not in param_value_dict[key]:
                    param_value_dict[key].append('channel')
                # force network and station aggreagtion for public method
                if 'public' in request.url:
                    if 'network' not in param_value_dict[key]:
                        param_value_dict[key].append('network')
                    if 'station' not in param_value_dict[key]:
                        param_value_dict[key].append('station')

    # make some parameters mandatory
    if 'start' not in param_value_dict and 'end' not in param_value_dict:
        raise LookupError
    # default parameters to be aggregated in restricted method: location, channel
    if 'restricted' in request.url and 'aggregate_on' not in param_value_dict:
        param_value_dict['aggregate_on'] = ['location', 'channel']
    # default parameters to be aggregated in public method: network, station, location, channel
    if 'public' in request.url and 'aggregate_on' not in param_value_dict:
        param_value_dict['aggregate_on'] = ['network', 'station', 'location', 'channel']
    # default output format: csv
    if ('restricted' in request.url or 'public' in request.url) and 'format' not in param_value_dict:
        param_value_dict['format'] = 'csv'

    log.debug('Final parameters: '+str(param_value_dict))
    return param_value_dict


def check_authentication(request):
    """
    Checks if user can be successfully authenticated
    Returns dictionary with token info if authentication is successful
    Returns dictionary with message if authentication is unsuccessful
    """

    log.info('Entering check_authentication')

    # verify signature
    gpg = gnupg.GPG(gnupghome=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'gnupghome'))
    verified = gpg.verify(request.body.decode())

    if verified:
        # extract and store token info in dictionary
        token = re.search(r"{(?P<token>.*)}", str(request.body)).groupdict()["token"]
        d = dict(
            [i for i in kv.split(":", 1)]
            for kv in token.replace('"', "").replace(" ", "").split(",")
        )
        if "givenName" not in d:
            d["givenName"] = d.get("cn", "null")
        log.debug('Token info: ' +str(d))

        # check if token has expired
        expiration_ts = datetime.strptime(d["valid_until"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if (expiration_ts - datetime.now()).total_seconds() < 0:
            return {'Failed_message': 'Token has expired!'}
        else:
            return d
    else:
        return {'Failed_message': 'Invalid token or no token file provided'}


@view_config(route_name='dataselectraw', openapi=True)
def raw(request):
    """
    Returns statistics to be used by computer
    Returns 400 bad request if invalid request parameter given
    Returns 401 unauthorized if authentication is unsuccessful
    """

    log.info(f"{request.method} {request.url}")
    if request.method != 'POST':
        return Response("<h1>405 Method Not Allowed</h1><p>Only POST method allowed</p>", status_code=405)

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return 400 bad request
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

    # check authentication
    # if authentication successful, return dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception:
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)
    else:
        pass

    log.info('Checked authentication')

    try:
        log.debug('Connecting to db, SELECT and FROM clause')
        session = Session()
        sqlreq = session.query(DataselectStat).join(Node).\
                            with_entities(DataselectStat.date, DataselectStat.network, DataselectStat.station, DataselectStat.location,\
                            DataselectStat.channel, DataselectStat.country, DataselectStat.nb_reqs, DataselectStat.nb_successful_reqs,\
                            DataselectStat.bytes, DataselectStat.clients, Node.name)

        # where clause
        log.debug('Making the WHERE clause')
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
    log.debug('Getting the results')
    results = []
    for row in sqlreq:
        rowToDict = DataselectStat.to_dict(row)
        rowToDict['datacenter'] = row.name
        results.append(rowToDict)

    # return json with metadata
    log.debug('Returning the results')
    return Response(text=json.dumps({'version': '1.0.0', 'request_parameters': request.query_string, 'results': results},
            default=str), content_type='application/json', charset='utf-8')


@view_config(route_name='dataselectrestricted', openapi=True)
def restricted(request):
    """
    Returns statistics to be read by human
    Returns 400 bad request if invalid request parameter given
    Returns 401 unauthorized if authentication is unsuccessful
    """

    log.info(f"{request.method} {request.url}")
    if request.method != 'POST':
        return Response("<h1>405 Method Not Allowed</h1><p>Only POST method allowed</p>", status_code=405)

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return 400 bad request
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

    # check authentication
    # if authentication successful, returns dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception:
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)
    else:
        pass

    log.info('Checked authentication')

    try:
        log.debug('Connecting to db, SELECT and FROM clause')
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
        log.debug('Making the WHERE clause')
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
        log.debug('Making the GROUP BY clause')
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
    log.debug('Getting the results')
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
        log.debug('Returning the results as JSON')
        return Response(text=json.dumps({'version': '1.0.0', 'matching': re.sub('&aggregate_on[^&]+', '', request.query_string),
                'aggregated_on': ','.join(param_value_dict['aggregate_on']), 'results': results}, default=str),
                content_type='application/json', charset='utf-8')
    else:
        log.debug('Returning the results as CSV')
        csvText = "# version: 1.0.0\n# matching: " + re.sub('&aggregate_on[^&]+', '', request.query_string) +\
            "\n# aggregated_on: " + ','.join(param_value_dict['aggregate_on']) +\
            "\nmonth,datacenter,network,station,location,channel,country,bytes,nb_reqs,nb_successful_reqs,clients"
        for res in results:
            csvText += '\n'
            for field in res:
                csvText += str(res[field]) + ','
            csvText = csvText[:-1]
        return Response(text=csvText, content_type='text/csv')


@view_config(route_name='dataselectpublic', request_method='GET', openapi=True)
def public(request):
    """
    Returns public statistics to be read by human
    Returns 400 bad request if invalid request parameter given
    """

    log.info(f"{request.method} {request.url}")

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return 400 bad request
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
        log.debug('Connecting to db, SELECT and FROM clause')
        session = Session()
        sqlreq = session.query(DataselectStat).join(Node).with_entities()

        # if aggregate on a parameter don't select it
        # instead return '*' for it meaning all matching instances of parameter
        if 'month' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.date)
        if 'datacenter' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(Node.name)
        if 'country' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.add_columns(DataselectStat.country)

        # fields to be summed up
        sqlreq = sqlreq.add_columns(func.sum(DataselectStat.nb_reqs).label('nb_reqs'),
                    func.sum(DataselectStat.nb_successful_reqs).label('nb_successful_reqs'),
                    func.sum(DataselectStat.bytes).label('bytes'), literal_column('#hll_union_agg(dataselect_stats.clients)').label('clients'))

        # where clause
        log.debug('Making the WHERE clause')
        if 'start' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date >= param_value_dict['start'])
        if 'end' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date <= param_value_dict['end'])
        if 'datacenter' in param_value_dict:
            sqlreq = sqlreq.filter(Node.name.in_(param_value_dict['datacenter']))
        if 'country' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.country.in_(param_value_dict['country']))

        # aggregate on requested parameters
        # group_by is the opposite process of the desired aggregation
        log.debug('Making the GROUP BY clause')
        if 'month' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.date)
        if 'datacenter' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(Node.name)
        if 'country' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.country)
        # force aggregation in network, station, location, channel parameters
        session.close()

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error or invalid SQL statement passed to database</p>", status_code=500)

    # get results as dictionaries
    # assign '*' at aggregated parameters
    log.debug('Getting the results')
    results = []
    for row in sqlreq:

        if row != (None, None, None, None):
            rowToDict = DataselectStat.to_dict_for_query(row)
            rowToDict['month'] = str(row.date)[:-3] if 'month' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['datacenter'] = row.name if 'datacenter' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['country'] = row.country if 'country' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['network'] = '*'
            rowToDict['station'] = '*'
            rowToDict['location'] = '*'
            rowToDict['channel'] = '*'
            results.append(rowToDict)

    # return json or csv with metadata
    if param_value_dict['format'] == 'json':
        log.debug('Returning the results as JSON')
        return Response(text=json.dumps({'version': '1.0.0', 'matching': re.sub('&aggregate_on[^&]+', '', request.query_string),
                'aggregated_on': ','.join(param_value_dict['aggregate_on']), 'results': results}, default=str),
                content_type='application/json', charset='utf-8')
    else:
        log.debug('Returning the results as CSV')
        csvText = "# version: 1.0.0\n# matching: " + re.sub('&aggregate_on[^&]+', '', request.query_string) +\
            "\n# aggregated_on: " + ','.join(param_value_dict['aggregate_on']) +\
            "\nmonth,datacenter,network,station,location,channel,country,bytes,nb_reqs,nb_successful_reqs,clients"
        for res in results:
            csvText += '\n'
            for field in res:
                csvText += str(res[field]) + ','
            csvText = csvText[:-1]
        return Response(text=csvText, content_type='text/csv')


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


@view_config(route_name='isrestricted', request_method='GET')
def isRestricted(request):
    """
    Returns whether a given network is restricted or not
    """

    log.info(f"{request.method} {request.url}")

    return Response("Not implemented yet!", status_code=200)

    '''
    try:
        session = Session()
        sqlreq = session.query(Network).with_entities(Network.node_id, Network.name).all()
        session.close()
        return Response(json={"restricted": "no"}, content_type='application/json')

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
    '''


@view_config(route_name='noderestriction')
def node_restriction_policy(request):
    """
    Returns new or existing default restriction policy of data center
    Returns 400 bad request if invalid request parameter given
    Returns 401 unauthorized if authentication is unsuccessful
    """

    log.info(f"{request.method} {request.url}")
    if request.method != 'POST':
        return Response("<h1>405 Method Not Allowed</h1><p>Only POST method allowed</p>", status_code=405)

    # check authentication
    # if authentication successful, return dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception:
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)
    else:
        pass

    log.info('Checked authentication')

    return Response("Not implemented yet!", status_code=200)


@view_config(route_name='networkrestriction')
def network_restriction_policy(request):
    """
    Returns new or existing state on whether network agrees with its data center restriction policy
    Returns 400 bad request if invalid request parameter given
    Returns 401 unauthorized if authentication is unsuccessful
    """

    log.info(f"{request.method} {request.url}")
    if request.method != 'POST':
        return Response("<h1>405 Method Not Allowed</h1><p>Only POST method allowed</p>", status_code=405)

    # check authentication
    # if authentication successful, return dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception:
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)
    else:
        pass

    log.info('Checked authentication')

    return Response("Not implemented yet!", status_code=200)
