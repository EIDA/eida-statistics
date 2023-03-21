from pyramid.response import Response
from pyramid.view import view_config
import os
import re
from ws_eidastats.model import Node, Network
from ws_eidastats.helper_functions import check_authentication, log, Session


@view_config(route_name='isrestricted', request_method='GET')
def isRestricted(request, internalCall=False, datacenter=None, network=None):
    """
    Returns whether a given network is restricted, open or if its restriction status is not yet defined
    """

    if internalCall:
        log.info('Entering isRestricted')
    else:
        log.info(f"{request.method} {request.url}")
        if any(x not in request.params for x in ['datacenter', 'network']):
            return Response(f"<h1>400 Bad Request</h1><p>Both 'datacenter' and 'network' parameters are required</p>", status_code=400)
        for key in request.params:
            log.debug('Parameter: '+key)
            if key not in ['datacenter', 'network']:
                return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter '{key}'</p>", status_code=400)
        log.info('Checked parameters')
        datacenter = request.params.get('datacenter')
        network = request.params.get('network')

    try:
        session = Session()
        sqlreq = session.query(Network).join(Node).with_entities(Node.restriction_policy, Network.inverted_policy, Network.eas_group)
        sqlreq = sqlreq.filter(Node.name == datacenter).filter(Network.name == network)
        session.close()

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq.first()
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given datacenter and network parameters</p>", status_code=400)
    if any(x is None for x in [row.restriction_policy, row.inverted_policy]):
        return Response(json={"restricted": "not yet defined", "group":row.eas_group}, content_type='application/json')
    elif int(row.restriction_policy)^int(row.inverted_policy):
        return Response(json={"restricted": "yes", "group":row.eas_group}, content_type='application/json')
    else:
        return Response(json={"restricted": "no", "group":row.eas_group}, content_type='application/json')


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

    # check parameters of request
    if 'datacenter' not in request.params:
        return Response(f"<h1>400 Bad Request</h1><p>'datacenter' parameter is required</p>", status_code=400)
    for key in request.params:
        log.debug('Parameter: '+key)
        if key not in ['datacenter', 'policy']:
            return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter '{key}'</p>", status_code=400)
        if key == 'policy' and request.params.get(key) not in ['0', '1']:
            return Response(f"<h1>400 Bad Request</h1><p>Unsupported value for parameter '{key}'</p>", status_code=400)

    log.info('Checked parameters')

    # check authentication
    # if authentication successful, return dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception:
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)

    log.info('Checked authentication')

    # check authorization
    # only a datacenter operator can perform this action regarding only their own datacenter
    memberOf = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).with_entities(Node.restriction_policy, Node.eas_group)
        sqlreq = sqlreq.filter(Node.name == request.params.get('datacenter'))
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq.first()
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No such datacenter</p>", status_code=400)

    if row.eas_group not in memberOf:
        return Response("<h1>403 Forbidden</h1><p>User has no authority to perform the requested action</p>", status_code=403)

    log.info('Checked authorization')

    # if policy parameter is specified, update the record in database and return the new policy
    if 'policy' in request.params:
        try:
            session = Session()
            sqlreq = session.query(Node).filter(Node.name == request.params.get('datacenter'))
            sqlreq = sqlreq.update({'restriction_policy': int(request.params.get('policy'))})
            session.commit()
            session.close()
        except Exception as e:
            log.error(str(e))
            return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

        return Response(json={"restriction_policy": request.params.get('policy')}, content_type='application/json')

    # if policy parameter not specified, return current policy from database
    return Response(json={"restriction_policy": "1" if row.restriction_policy else "0" if row.restriction_policy is not None else None},
            content_type='application/json')


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

    # check parameters of request
    if any(x not in request.params for x in ['datacenter', 'network']):
        return Response(f"<h1>400 Bad Request</h1><p>Both 'datacenter' and 'network' parameters are required</p>", status_code=400)
    for key in request.params:
        log.debug('Parameter: '+key)
        if key not in ['datacenter', 'network', 'invert_policy', 'eas_group']:
            return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter '{key}'</p>", status_code=400)
        if key == 'invert_policy' and request.params.get(key) not in ['0', '1']:
            return Response(f"<h1>400 Bad Request</h1><p>Unsupported value for parameter '{key}'</p>", status_code=400)

    log.info('Checked parameters')

    # check authentication
    # if authentication successful, return dictionary with token info
    # else return 401 unauthorized
    try:
        tokenDict = check_authentication(request)
    except Exception:
        return Response("<h1>401 Unauthorized</h1><p>Malformed token file provided</p>", status_code=401)
    if 'Failed_message' in tokenDict:
        return Response(f"<h1>401 Unauthorized</h1><p>{tokenDict['Failed_message']}</p>", status_code=401)

    log.info('Checked authentication')

    # check authorization
    # only a datacenter operator can perform this action regarding only their own datacenter
    memberOf = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).join(Network)
        sqlreq = sqlreq.with_entities(Network.inverted_policy, Network.eas_group, Node.eas_group, Node.id)
        sqlreq = sqlreq.filter(Node.name == request.params.get('datacenter')).filter(Network.name == request.params.get('network'))
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq.first()
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given datacenter and network parameters</p>", status_code=400)

    if row.eas_group not in memberOf:
        return Response("<h1>403 Forbidden</h1><p>User has no authority to perform the requested action</p>", status_code=403)

    log.info('Checked authorization')

    # if invert_policy or eas_group parameters are specified, update the record in database and return the new inverted_policy and eas_group
    if any(x in request.params for x in ['invert_policy', 'eas_group']):
        update_dict = {}
        if 'invert_policy' in request.params:
            update_dict['invert_policy'] = int(request.params.get('invert_policy'))
        if 'eas_group' in request.params:
            update_dict['eas_group'] = request.params.get('eas_group')

        # TODO: if network is about to be restricted force specifying eas_group parameter as well

        try:
            session = Session()
            sqlreq = session.query(Network).filter(Network.node_id == row.id).filter(Network.name == request.params.get('network'))
            sqlreq.update(update_dict)
            session.commit()
            # query database to return updated record
            resp = sqlreq.first()
            session.close()
        except Exception as e:
            log.error(str(e))
            return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

        return Response(json={"invert_policy": resp.inverted_policy, "eas_group": res.eas_group}, content_type='application/json')

    # if none of the invert_policy or eas_group parameters is specified, return current inverted_policy and eas_group from database
    return Response(json={"invert_policy": "1" if row.inverted_policy else "0" if row.inverted_policy is not None else None,
            "eas_group": row.eas_group}, content_type='application/json')
