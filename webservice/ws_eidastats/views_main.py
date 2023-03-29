from pyramid.response import Response
from pyramid.view import view_config
from pyramid.view import notfound_view_config
import os
import json
import re
from ws_eidastats.model import Node, DataselectStat
from ws_eidastats.helper_functions import check_authentication, check_request_parameters, NoNetwork, Mandatory, NoDatacenterAndNetwork, log, Session
from ws_eidastats.views_restrictions import isRestricted
from sqlalchemy import or_
from sqlalchemy.sql import func
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
    # different behavior depending on whether user is data center operator or not
    memberOf = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).with_entities(Node.eas_group)
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    operator = False
    for row in sqlreq:
        if row.eas_group in memberOf:
            operator = True
            log.info('User is data center operator')
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
    except NoDatacenterAndNetwork:
        return Response("<h1>400 Bad Request</h1><p>For non-operator users, both 'datacenter' and 'network' parameters are required</p>", status_code=400)
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    # if user is not operator can only get statistics for open networks or networks to which they have access
    if not operator:
        # if network is specified, check if network is open or restricted
        if 'network' in param_value_dict:
            restricted = isRestricted(request, internalCall=True, datacenter=param_value_dict['datacenter'][0], network=param_value_dict['network'][0])
            if restricted.status_code == 500:
                return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
            elif restricted.status_code == 400:
                return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given datacenter and network parameters</p>", status_code=400)
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
    # different behavior depending on whether user is data center operator or not
    memberOf = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).with_entities(Node.eas_group)
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    operator = False
    for row in sqlreq:
        if row.eas_group in memberOf:
            operator = True
            log.info('User is data center operator')
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
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    # if user is not operator can only get statistics below datacenter level for open networks or networks to which they have access
    if not operator:
        # if network is specified, check if network is open or restricted
        if 'network' in param_value_dict:
            restricted = isRestricted(request, internalCall=True, datacenter=param_value_dict['datacenter'][0], network=param_value_dict['network'][0])
            if restricted.status_code == 500:
                return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
            elif restricted.status_code == 400:
                return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given datacenter and network parameters</p>", status_code=400)
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
    if param_value_dict.get('format') == 'json':
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
    except Mandatory:
        return Response("<h1>400 Bad Request</h1><p>Specify at least 'start' parameter</p>", status_code=400)
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1>", status_code=500)

    log.info('Checked parameters of request')

    # if network is specified, check if network is open or restricted
    if 'network' in param_value_dict:
        restricted = isRestricted(request, internalCall=True, datacenter=param_value_dict['datacenter'][0], network=param_value_dict['network'][0])
        if restricted.status_code == 500:
            return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)
        elif restricted.status_code == 400:
            return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given datacenter and network parameters</p>", status_code=400)
        elif restricted.json['restricted'] == 'yes':
            log.debug('Network is restricted')
            return Response("<h1>401 Unauthorized</h1><p>No access to restricted networks for non-authenticated users<br>"+\
                    "If you are a member of EIDA consider using /restricted method instead</p>", status_code=401)

    log.info('Checked network restriction')

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
        if 'network' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.network.in_(param_value_dict['network']))
        if 'country' in param_value_dict:
            sqlreq = sqlreq.filter(DataselectStat.country.in_(param_value_dict['country']))

        # aggregate on requested parameters
        # group_by is the opposite process of the desired aggregation
        log.debug('Making the GROUP BY clause')
        if 'month' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.date)
        if 'datacenter' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(Node.name)
        if 'network' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.network)
        if 'country' not in param_value_dict['aggregate_on']:
            sqlreq = sqlreq.group_by(DataselectStat.country)
        # force aggregation in station, location, channel parameters
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
            rowToDict['country'] = row.country if 'country' not in param_value_dict['aggregate_on'] else '*'
            rowToDict['station'] = '*'
            rowToDict['location'] = '*'
            rowToDict['channel'] = '*'
            results.append(rowToDict)

    # return json or csv with metadata
    if param_value_dict.get('format') == 'json':
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
