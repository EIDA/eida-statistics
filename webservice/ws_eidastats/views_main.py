from pyramid.response import Response
from pyramid.view import view_config
from pyramid.view import notfound_view_config
import os
import json
import re
import python_hll
from python_hll.util import NumberUtil
from python_hll.hll import HLL
from ws_eidastats.model import Node, DataselectStat
from ws_eidastats.helper_functions import get_nodes, check_authentication, check_request_parameters, log, Session
from ws_eidastats.helper_functions import NoNetwork, Mandatory, NoNodeAndNetwork, BothMonthYear
from ws_eidastats.views_restrictions import isRestricted
from sqlalchemy import or_
from sqlalchemy.sql import func, extract
from sqlalchemy.sql.expression import literal_column


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


@view_config(route_name='dataselectraw', openapi=True)
def raw(request):
    """
    Returns statistics to be used by computer
    Returns 400 bad request if invalid request parameter given
    Returns 401 unauthorized if authentication is unsuccessful
    Returns 403 forbidden if user has no access to restricted network
    """

    log.info(f"{request.method} {request.url}")
    if request.method != 'POST':
        return Response("<h1>405 Method Not Allowed</h1><p>Only POST method allowed</p>", status_code=405)

    # check authentication
    # if authentication successful, return dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception as e:
        log.error(str(e))
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)

    log.info('Checked authentication')

    # check authorization
    # different behavior depending on whether user is node operator or not
    memberOf = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).with_entities(Node.eas_group).all()
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    operator = False
    for row in sqlreq:
        if row.eas_group in memberOf:
            operator = True
            log.info('User is node operator')
            break

    log.info('Checked authorization')

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return 400 bad request
    try:
        if operator:
            param_value_dict = check_request_parameters(request, one_network=False)
        else:
            param_value_dict = check_request_parameters(request)
    except KeyError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter {str(e)}</p>", status_code=400)
    except ValueError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Unsupported value for parameter '{str(e)}'</p>", status_code=400)
    except Mandatory:
        return Response("<h1>400 Bad Request</h1><p>Specify at least 'start' parameter</p>", status_code=400)
    except NoNodeAndNetwork:
        return Response("<h1>400 Bad Request</h1><p>For non-operator users, both 'node' and 'network' parameters are required</p>", status_code=400)
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    # if user is not operator can only get statistics for open networks or networks to which they have access
    if not operator:
        # if network is specified, check if network is open or restricted
        if 'network' in param_value_dict:
            restricted = isRestricted(request, internalCall=True, node=param_value_dict['node'][0], network=param_value_dict['network'][0])
            if restricted.status_code == 500:
                return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
            elif restricted.status_code == 400:
                return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given node and network parameters</p>", status_code=400)
            # if network is restricted, check if user has access
            elif restricted.json['restricted'] == 'yes':
                log.debug('Network is restricted. Checking if user has access')
                if restricted.json['group'] not in memberOf:
                    log.info('User has no access')
                    return Response("<h1>403 Forbidden</h1><p>User has no access to the requested network</p>", status_code=403)

        log.info('Checked network restriction. User can access')

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
        if 'node' in param_value_dict:
            sqlreq = sqlreq.filter(Node.name.in_(param_value_dict['node']))
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

    # get results as dictionaries and add node name
    log.debug('Getting the results')
    results = []
    for row in sqlreq:
        rowToDict = DataselectStat.to_dict(row)
        rowToDict['node'] = row.name
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
    Returns 403 forbidden if user has no access to restricted network
    """

    log.info(f"{request.method} {request.url}")
    if request.method != 'POST':
        return Response("<h1>405 Method Not Allowed</h1><p>Only POST method allowed</p>", status_code=405)

    # check authentication
    # if authentication successful, return dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception as e:
        log.error(str(e))
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)

    log.info('Checked authentication')

    # check authorization
    # different behavior depending on whether user is node operator or not
    memberOf = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).with_entities(Node.eas_group).all()
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    operator = False
    for row in sqlreq:
        if row.eas_group in memberOf:
            operator = True
            log.info('User is node operator')
            break

    log.info('Checked authorization')

    # check parameters and values
    # return dictionary with parameters and values if acceptable
    # otherwise catch error and return 400 bad request
    try:
        if operator:
            param_value_dict = check_request_parameters(request, one_network=False)
        else:
            param_value_dict = check_request_parameters(request)
    except KeyError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter {str(e)}</p>", status_code=400)
    except ValueError as e:
        return Response(f"<h1>400 Bad Request</h1><p>Unsupported value for parameter '{str(e)}'</p>", status_code=400)
    except Mandatory:
        return Response("<h1>400 Bad Request</h1><p>Specify at least 'start' parameter</p>", status_code=400)
    except NoNetwork:
        return Response("<h1>400 Bad Request</h1><p>For non-operator users, 'network' parameter is required below network level"+\
                " or whenever any of the 'station', 'location', 'channel' parameters are specified</p>", status_code=400)
    except BothMonthYear:
        return Response("<h1>400 Bad Request</h1><p>Only one of 'month' or 'year' details can be requested each time</p>", status_code=400)
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    # if user is not operator and network is specified, check if either network is open or user has access to it at least in one node
    if not operator and 'network' in param_value_dict:
        access = False
        noEntry = True
        nodes = param_value_dict.get('node')
        # if no node is specified, get all available nodes from database
        if nodes is None:
            try:
                nodes = [node['name'] for node in get_nodes(request, internalCall=True).json['nodes']]
            except Exception as e:
                raise Exception(e)
        for n in nodes:
            restricted = isRestricted(request, internalCall=True, node=n, network=param_value_dict['network'][0])
            if restricted.status_code == 500:
                return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
            if restricted.status_code == 400:
                continue
            else:
                noEntry = False
            if restricted.json['restricted'] == 'no':
                log.debug('Network is open at least in one node')
                access = True
                break
            # if network is restricted, check if user has access
            elif restricted.json['restricted'] == 'yes' and restricted.json['group'] in memberOf:
                log.info('User can access restricted network')
                access = True
                break
        if noEntry:
            return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given node and network parameters</p>", status_code=400)
        if not access:
            log.debug('Network is restricted and user has no access')
            return Response("<h1>403 Forbidden</h1><p>User has no access to the requested network</p>", status_code=403)

        log.info('Checked network restriction')

    try:
        log.debug('Connecting to db, SELECT and FROM clause')
        session = Session()
        sqlreq = session.query(DataselectStat).join(Node).with_entities()

        # select needed columns depending on level and details
        # return '*' for not selected columns meaning all matching instances
        if 'level' in param_value_dict:
            sqlreq = sqlreq.add_columns(Node.name)
        if param_value_dict.get('level') in ['network', 'station', 'location', 'channel']:
            sqlreq = sqlreq.add_columns(DataselectStat.network)
        if param_value_dict.get('level') in ['station', 'location', 'channel']:
            sqlreq = sqlreq.add_columns(DataselectStat.station)
        if param_value_dict.get('level') in ['location', 'channel']:
            sqlreq = sqlreq.add_columns(DataselectStat.location)
        if param_value_dict.get('level') == 'channel':
            sqlreq = sqlreq.add_columns(DataselectStat.channel)
        if 'month' in param_value_dict['details']:
            sqlreq = sqlreq.add_columns(DataselectStat.date)
        elif 'year' in param_value_dict['details']:
            sqlreq = sqlreq.add_columns(extract('year', DataselectStat.date).label('year'))
        if 'country' in param_value_dict['details']:
            sqlreq = sqlreq.add_columns(DataselectStat.country)

        # fields to be summed up
        sqlreq = sqlreq.add_columns(func.sum(DataselectStat.nb_reqs).label('nb_reqs'),
                    func.sum(DataselectStat.nb_successful_reqs).label('nb_successful_reqs'),
                    func.sum(DataselectStat.bytes).label('bytes'), literal_column('hll_union_agg(dataselect_stats.clients)').label('clients'))

        # where clause
        log.debug('Making the WHERE clause')
        if 'start' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date >= param_value_dict['start'])
        if 'end' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date <= param_value_dict['end'])
        if 'node' in param_value_dict:
            sqlreq = sqlreq.filter(Node.name.in_(param_value_dict['node']))
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

        # group_by clause for the details
        log.debug('Making the GROUP BY clause')
        if 'level' in param_value_dict:
            sqlreq = sqlreq.group_by(Node.name)
        if param_value_dict.get('level') in ['network', 'station', 'location', 'channel']:
            sqlreq = sqlreq.group_by(DataselectStat.network)
        if param_value_dict.get('level') in ['station', 'location', 'channel']:
            sqlreq = sqlreq.group_by(DataselectStat.station)
        if param_value_dict.get('level') in ['location', 'channel']:
            sqlreq = sqlreq.group_by(DataselectStat.location)
        if param_value_dict.get('level') == 'channel':
            sqlreq = sqlreq.group_by(DataselectStat.channel)
        if 'month' in param_value_dict['details']:
            sqlreq = sqlreq.group_by(DataselectStat.date)
        elif 'year' in param_value_dict['details']:
            sqlreq = sqlreq.group_by('year')
        if 'country' in param_value_dict['details']:
            sqlreq = sqlreq.group_by(DataselectStat.country)
        session.close()

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error or invalid SQL statement passed to database</p>", status_code=500)

    # get results as dictionaries
    # assign '*' at aggregated parameters
    log.debug('Getting the results')
    results = []
    # make a result item for networks that user cannot access
    atLeastOneNoAccess = False
    if not operator:
        results.append({'date':'*', 'node':'Other', 'network':'Other', 'country':'*', 'station':'*', 'location':'*', 'channel':'*',
            'bytes': 0, 'nb_reqs': 0, 'nb_successful_reqs': 0, 'clients': HLL(11,5)})
    for row in sqlreq:
        if row != (None, None, None, None):
            rowToDict = DataselectStat.to_dict_for_human(row)
            if not operator:
                # if below datacenter level, check for networks that user has no access and group them in the no-access networks result item
                if param_value_dict.get('level') in ['network', 'station', 'location', 'channel']:
                    # first check if network is open
                    restricted = isRestricted(request, internalCall=True, node=row.name, network=row.network)
                    if restricted.status_code != 200:
                        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
                    elif restricted.json['restricted'] == 'yes' and restricted.json['group'] not in memberOf:
                        log.debug('Grouping network as non-accessable in results')
                        results[0]['bytes'] += int(row.bytes)
                        results[0]['nb_reqs'] += row.nb_reqs
                        results[0]['nb_successful_reqs'] += row.nb_successful_reqs
                        results[0]['clients'].union(HLL.from_bytes(NumberUtil.from_hex(row.clients[2:], 0, len(row.clients[2:]))))
                        atLeastOneNoAccess = True
                        continue

            rowToDict = DataselectStat.to_dict_for_human(row)
            rowToDict['date'] = str(row.date)[:-3] if 'month' in param_value_dict['details'] else\
                                        str(row.year)[:4] if 'year' in param_value_dict['details'] else '*'
            rowToDict['node'] = row.name if 'level' in param_value_dict else '*'
            rowToDict['network'] = row.network if param_value_dict.get('level') in ['network', 'station', 'location', 'channel'] else '*'
            rowToDict['station'] = row.station if param_value_dict.get('level') in ['station', 'location', 'channel'] else '*'
            rowToDict['location'] = row.location if param_value_dict.get('level') in ['location', 'channel'] else '*'
            rowToDict['channel'] = row.channel if param_value_dict.get('level') == 'channel' else '*'
            rowToDict['country'] = row.country if 'country' in param_value_dict['details'] else '*'
            rowToDict['clients'] = HLL.from_bytes(NumberUtil.from_hex(row.clients[2:], 0, len(row.clients[2:]))).cardinality()
            results.append(rowToDict)

    # remove non-accessable networks result item if no such network was returned
    if not atLeastOneNoAccess:
        results.pop(0)
    else:
        results[0]['clients'] = results[0]['clients'].cardinality()

    # sort results by date
    if 'details' in param_value_dict and any(x in param_value_dict['details'] for x in ['month', 'year']):
        results = sorted(results, key=lambda x: x['date'])

    # return json or csv with metadata
    if param_value_dict.get('format') == 'json':
        log.debug('Returning the results as JSON')
        return Response(text=json.dumps({'version': '1.0.0', 'request_parameters': request.query_string, 'results': results}, default=str),
                content_type='application/json', charset='utf-8')
    else:
        log.debug('Returning the results as CSV')
        csvText = "# version: 1.0.0\n# request_parameters: " + request.query_string +\
            "\ndate,node,network,station,location,channel,country,bytes,nb_reqs,nb_successful_reqs,clients"
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
    Returns 401 unauthorized if network is restricted
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
    except Mandatory:
        return Response("<h1>400 Bad Request</h1><p>Specify at least 'start' parameter</p>", status_code=400)
    except BothMonthYear:
        return Response("<h1>400 Bad Request</h1><p>Only one of 'month' or 'year' details can be requested each time</p>", status_code=400)
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    # if network is specified, check if network is open at least in one node or restricted in all nodes
    if 'network' in param_value_dict:
        open = False
        noEntry = True
        nodes = param_value_dict.get('node')
        # if no node is specified, get all available nodes from database
        if nodes is None:
            try:
                nodes = [node['name'] for node in get_nodes(request, internalCall=True).json['nodes']]
            except Exception as e:
                raise Exception(e)
        for n in nodes:
            restricted = isRestricted(request, internalCall=True, node=n, network=param_value_dict['network'][0])
            if restricted.status_code == 500:
                return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
            if restricted.status_code == 400:
                continue
            else:
                noEntry = False
            if restricted.json['restricted'] == 'no':
                log.debug('Network is open at least in one node')
                open = True
                break
        if noEntry:
            return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given node and network parameters</p>", status_code=400)
        if not open:
            log.debug('Network is restricted')
            return Response("<h1>401 Unauthorized</h1><p>No access to restricted networks for non-authenticated users<br>"+\
                    "If you are a member of EIDA consider using /restricted method instead</p>", status_code=401)

        log.info('Checked network restriction')

    try:
        log.debug('Connecting to db, SELECT and FROM clause')
        session = Session()
        sqlreq = session.query(DataselectStat).join(Node).with_entities()

        # select needed columns depending on level and details
        # return '*' for not selected columns meaning all matching instances
        if 'level' in param_value_dict:
            sqlreq = sqlreq.add_columns(Node.name)
        if param_value_dict.get('level') == 'network':
            sqlreq = sqlreq.add_columns(DataselectStat.network)
        if 'month' in param_value_dict['details']:
            sqlreq = sqlreq.add_columns(DataselectStat.date)
        elif 'year' in param_value_dict['details']:
            sqlreq = sqlreq.add_columns(extract('year', DataselectStat.date).label('year'))
        if 'country' in param_value_dict['details']:
            sqlreq = sqlreq.add_columns(DataselectStat.country)

        # fields to be summed up
        sqlreq = sqlreq.add_columns(func.sum(DataselectStat.nb_reqs).label('nb_reqs'),
                    func.sum(DataselectStat.nb_successful_reqs).label('nb_successful_reqs'),
                    func.sum(DataselectStat.bytes).label('bytes'), literal_column('hll_union_agg(dataselect_stats.clients)').label('clients'))

        # where clause
        log.debug('Making the WHERE clause')
        if 'start' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date >= param_value_dict['start'])
        if 'end' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.date <= param_value_dict['end'])
        if 'node' in param_value_dict:
            sqlreq = sqlreq.filter(Node.name.in_(param_value_dict['node']))
        if 'network' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.network.in_(param_value_dict['network']))
        if 'country' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.country.in_(param_value_dict['country']))

        # group_by clause for the details
        log.debug('Making the GROUP BY clause')
        if 'level' in param_value_dict:
            sqlreq = sqlreq.group_by(Node.name)
        if param_value_dict.get('level') == 'network':
            sqlreq = sqlreq.group_by(DataselectStat.network)
        if 'month' in param_value_dict['details']:
            sqlreq = sqlreq.group_by(DataselectStat.date)
        elif 'year' in param_value_dict['details']:
            sqlreq = sqlreq.group_by('year')
        if 'country' in param_value_dict['details']:
            sqlreq = sqlreq.group_by(DataselectStat.country)
        session.close()

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error or invalid SQL statement passed to database</p>", status_code=500)

    # get results as dictionaries
    # assign '*' at non-selected columns
    log.debug('Getting the results')
    results = []
    # make a result item for restricted networks
    results.append({'date':'*', 'node':'Other', 'network':'Other', 'country':'*', 'station':'*', 'location':'*', 'channel':'*',
        'bytes': 0, 'nb_reqs': 0, 'nb_successful_reqs': 0, 'clients': HLL(11,5)})
    atLeastOneRestricted = False
    for row in sqlreq:
        if row != (None, None, None, None):
            rowToDict = DataselectStat.to_dict_for_human(row)
            # if below datacenter level, check for restricted networks and group them in the restricted networks result item
            if param_value_dict.get('level') == 'network':
                restricted = isRestricted(request, internalCall=True, node=row.name, network=row.network)
                if restricted.status_code != 200:
                    return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
                elif restricted.json['restricted'] == 'yes':
                    log.debug('Grouping network as restricted in results')
                    results[0]['bytes'] += int(row.bytes)
                    results[0]['nb_reqs'] += row.nb_reqs
                    results[0]['nb_successful_reqs'] += row.nb_successful_reqs
                    results[0]['clients'].union(HLL.from_bytes(NumberUtil.from_hex(row.clients[2:], 0, len(row.clients[2:]))))
                    atLeastOneRestricted = True
                    continue

            rowToDict['date'] = str(row.date)[:-3] if 'month' in param_value_dict['details'] else\
                                        str(row.year)[:4] if 'year' in param_value_dict['details'] else '*'
            rowToDict['node'] = row.name if 'level' in param_value_dict else '*'
            rowToDict['network'] = row.network if param_value_dict.get('level') == 'network' else '*'
            rowToDict['country'] = row.country if 'country' in param_value_dict['details'] else '*'
            rowToDict['station'] = '*'
            rowToDict['location'] = '*'
            rowToDict['channel'] = '*'
            rowToDict['clients'] = HLL.from_bytes(NumberUtil.from_hex(row.clients[2:], 0, len(row.clients[2:]))).cardinality()
            results.append(rowToDict)

    # remove restricted networks result item if no restricted network was returned
    if not atLeastOneRestricted:
        results.pop(0)
    else:
        results[0]['clients'] = results[0]['clients'].cardinality()

    # sort results by date
    if 'details' in param_value_dict and any(x in param_value_dict['details'] for x in ['month', 'year']):
        results = sorted(results, key=lambda x: x['date'])

    # return json or csv with metadata
    if param_value_dict.get('format') == 'json':
        log.debug('Returning the results as JSON')
        return Response(text=json.dumps({'version': '1.0.0', 'request_parameters': request.query_string, 'results': results}, default=str),
                content_type='application/json', charset='utf-8')
    else:
        log.debug('Returning the results as CSV')
        csvText = "# version: 1.0.0\n# request_parameters: " + request.query_string +\
            "\ndate,node,network,station,location,channel,country,bytes,nb_reqs,nb_successful_reqs,clients"
        for res in results:
            csvText += '\n'
            for field in res:
                csvText += str(res[field]) + ','
            csvText = csvText[:-1]
        return Response(text=csvText, content_type='text/csv')
