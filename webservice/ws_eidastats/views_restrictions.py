from pyramid.response import Response
from pyramid.view import view_config
import os
import logging
import re
from ws_eidastats.model import Node, Network
from ws_eidastats.helper_functions import check_authentication
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


log = logging.getLogger(__name__)
dbURI = os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')
engine = create_engine(dbURI)
Session = sessionmaker(engine)


@view_config(route_name='isrestricted', request_method='GET')
def isRestricted(request):
    """
    Returns whether a given network is restricted, open or if its restriction status is not yet defined
    """

    log.info(f"{request.method} {request.url}")

    if any(x not in request.params for x in ['datacenter', 'network']):
        return Response(f"<h1>400 Bad Request</h1><p>Both 'datacenter' and 'network' parameters are required</p>", status_code=400)
    for key in request.params:
        log.debug('Parameter: '+key)
        if key not in ['datacenter', 'network']:
            return Response(f"<h1>400 Bad Request</h1><p>Invalid parameter '{key}'</p>", status_code=400)

    log.info('Checked parameters')

    try:
        session = Session()
        sqlreq = session.query(Network).join(Node).with_entities(Node.restriction_policy.label('restriction_policy'), Network.inverted_policy.label('inverted_policy'))
        sqlreq = sqlreq.filter(Node.name == request.params.get('datacenter')).filter(Network.name == request.params.get('network'))
        session.close()

    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq.first()
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given datacenter and network parameters</p>", status_code=400)
    if any(x is None for x in [row.restriction_policy, row.inverted_policy]):
        return Response(json={"restricted": "not yet defined"}, content_type='application/json')
    elif int(row.restriction_policy)^int(row.inverted_policy):
        return Response(json={"restricted": "yes"}, content_type='application/json')
    else:
        return Response(json={"restricted": "no"}, content_type='application/json')


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
    isMember = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).with_entities(Node.restriction_policy.label('restriction_policy'), Node.eas_group.label('eas_group'))
        sqlreq = sqlreq.filter(Node.name == request.params.get('datacenter'))
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq.first()
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No such datacenter</p>", status_code=400)

    if row.eas_group not in isMember:
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
    if row.restriction_policy is None:
        return Response(json={"restriction_policy": "not yet defined"}, content_type='application/json')
    elif not row.restriction_policy:
        return Response(json={"restriction_policy": "0"}, content_type='application/json')
    elif row.restriction_policy:
        return Response(json={"restriction_policy": "1"}, content_type='application/json')


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
        if key not in ['datacenter', 'network', 'invert_policy']:
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
    isMember = re.findall(r'/epos/(\w+)', tokenDict['memberof'])
    try:
        session = Session()
        sqlreq = session.query(Node).join(Network)
        sqlreq = sqlreq.with_entities(Network.inverted_policy.label('inverted_policy'), Node.eas_group.label('eas_group'), Node.id.label('id'))
        sqlreq = sqlreq.filter(Node.name == request.params.get('datacenter')).filter(Network.name == request.params.get('network'))
        session.close()
    except Exception as e:
        log.error(str(e))
        return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

    row = sqlreq.first()
    if not row:
        return Response(f"<h1>400 Bad Request</h1><p>No entry that matches given datacenter and network parameters</p>", status_code=400)

    if row.eas_group not in isMember:
        return Response("<h1>403 Forbidden</h1><p>User has no authority to perform the requested action</p>", status_code=403)

    log.info('Checked authorization')

    # if invert_policy parameter is specified, update the record in database and return the new inverted_policy
    if 'invert_policy' in request.params:
        try:
            session = Session()
            sqlreq = session.query(Network).filter(Network.node_id == row.id).filter(Network.name == request.params.get('network'))
            sqlreq = sqlreq.update({'inverted_policy': int(request.params.get('invert_policy'))})
            session.commit()
            session.close()
        except Exception as e:
            log.error(str(e))
            return Response("<h1>500 Internal Server Error</h1><p>Database connection error</p>", status_code=500)

        return Response(json={"invert_policy": request.params.get('invert_policy')}, content_type='application/json')

    # if invert_policy parameter not specified, return current inverted_policy from database
    if row.inverted_policy is None:
        return Response(json={"invert_policy": "not yet defined"}, content_type='application/json')
    elif not row.inverted_policy:
        return Response(json={"invert_policy": "0"}, content_type='application/json')
    elif row.inverted_policy:
        return Response(json={"invert_policy": "1"}, content_type='application/json')
