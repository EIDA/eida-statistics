from pyramid.response import Response
from pyramid.view import view_config
import os
import re
from ws_eidastats.model import Node, Network
from ws_eidastats.helper_functions import check_authentication, log, Session


@view_config(route_name='isrestricted', request_method='GET')
def isRestricted(request, internalCall=False, node=None, network=None):
    """
    Returns whether a given network is restricted, open or if its restriction status is not yet defined
    """

    if internalCall:
        log.info('Entering isRestricted')
    else:
        log.info(f"{request.method} {request.url}")
        if any(x not in request.params for x in ['node', 'network']):
            return Response(f"<h1>400 Bad Request</h1><p>Both 'node' and 'network' parameters are required</p>", status_code=400)
        for key in request.params:
            log.debug('Parameter: '+key)
            if key not in ['node', 'network']:
                return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter '{key}'</p>", status_code=400)
        log.info('Checked parameters')
        node = request.params.get('node')
        network = request.params.get('network')

    try:
        session = Session()
        sqlreq = session.query(Network).join(Node).with_entities(Node.restriction_policy, Network.inverted_policy, Network.eas_group)
        sqlreq = sqlreq.filter(Node.name == node).filter(Network.name == network).first()
        session.close()

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given node and network parameters</p>", status_code=400)
    if any(x is None for x in [row.restriction_policy, row.inverted_policy]):
        return Response(json={"restricted": "not yet defined", "group":row.eas_group}, content_type='application/json')
    elif int(row.restriction_policy)^int(row.inverted_policy):
        return Response(json={"restricted": "yes", "group":row.eas_group}, content_type='application/json')
    else:
        return Response(json={"restricted": "no", "group":row.eas_group}, content_type='application/json')


@view_config(route_name='noderestriction', request_method='GET')
def node_restriction_policy(request):
    """
    Returns existing default restriction policy of node
    Returns 400 bad request if invalid request parameter given
    """

    log.info(f"{request.method} {request.url}")

    # check parameters of request
    if 'node' not in request.params:
        return Response(f"<h1>400 Bad Request</h1><p>'node' parameter is required</p>", status_code=400)
    for key in request.params:
        log.debug('Parameter: '+key)
        if key != 'node':
            return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter '{key}'</p>", status_code=400)

    log.info('Checked parameters')

    try:
        session = Session()
        sqlreq = session.query(Node).filter(Node.name == request.params.get('node')).first()
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given node parameter</p>", status_code=400)

    return Response(json={"restriction_policy": "1" if row.restriction_policy else "0" if row.restriction_policy is not None else None},
            content_type='application/json')


@view_config(route_name='networkrestriction', request_method='GET')
def network_restriction_policy(request):
    """
    Returns existing state on whether network agrees with its node restriction policy
    Returns 400 bad request if invalid request parameter given
    """

    log.info(f"{request.method} {request.url}")

    # check parameters of request
    if any(x not in request.params for x in ['node', 'network']):
        return Response(f"<h1>400 Bad Request</h1><p>Both 'node' and 'network' parameters are required</p>", status_code=400)
    for key in request.params:
        log.debug('Parameter: '+key)
        if key not in ['node', 'network']:
            return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter '{key}'</p>", status_code=400)

    log.info('Checked parameters')

    try:
        session = Session()
        sqlreq = session.query(Node).join(Network).with_entities(Network.inverted_policy, Network.eas_group)
        sqlreq = sqlreq.filter(Node.name == request.params.get('node')).filter(Network.name == request.params.get('network')).first()
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given node and network parameters</p>", status_code=400)

    return Response(json={"invert_policy": "1" if row.inverted_policy else "0" if row.inverted_policy is not None else None,
            "eas_group": row.eas_group}, content_type='application/json')
