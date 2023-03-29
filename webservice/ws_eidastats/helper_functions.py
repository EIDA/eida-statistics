from pyramid.response import Response
from pyramid.view import view_config
from datetime import datetime
import gnupg
import re
import os
import logging
from ws_eidastats.model import Node
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


log = logging.getLogger(__name__)
dbURI = os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')
engine = create_engine(dbURI)
Session = sessionmaker(engine)


class NoNetwork(Exception):
    "Raised when network parameter must be specified"
    pass

class NoDatacenterAndNetwork(Exception):
    "Raised when both datacenter and network parameterss must be specified"
    pass

class Mandatory(Exception):
    "Raised when mandatory parameters are not specified"
    pass


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


def check_request_parameters(request, one_network=True):
    """
    Checks if parameters and values given in a request are acceptable
    Returns dictionary with parameters and their values if acceptable
    Raises error if not acceptable
    """

    log.info('Entering check_request_parameters')

    # parameters that all methods accept
    accepted = ['start', 'end', 'datacenter', 'network', 'country']
    # parameters accepted by raw method
    if 'raw' in request.url:
        accepted += ['station', 'location', 'channel']
    # parameters accepted by restricted method
    elif 'restricted' in request.url:
        accepted += ['station', 'location', 'channel', 'level', 'details', 'format']
    # parameters accepted by public method
    elif 'public' in request.url:
        accepted += ['level', 'details', 'format']

    params = request.params
    # make start parameter mandatory
    if 'start' not in params:
        raise Mandatory
    # if /raw method is used and user is not a data center operator, then both datacenter and network parameters must be specified
    if 'raw' in request.url and one_network and any(x not in params for x in ['datacenter', 'network']):
        raise NoDatacenterAndNetwork
    # if /restricted method is used and user is not a data center operator and specifies any of station, channel, location parameters
    # then network parameter must be specified
    if 'restricted' in request.url and one_network and any(x in params for x in ['station', 'channel', 'location']) and 'network' not in params:
        raise NoNetwork
    param_value_dict = {}

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
        elif key == 'level':
            # level acceptable values depending on the method
            log.debug('Level: '+params.get(key))
            if params.get(key) not in ['datacenter', 'network', 'station', 'location', 'channel']:
                raise ValueError(key)
            elif 'public' in request.url and params.get(key) not in ['datacenter', 'network']:
                raise ValueError(key)
            else:
                # if /restricted method is used and user is not a data center operator and level is below network
                # then network parameter must be specified
                if 'restricted' in request.url and one_network and params.get(key) in ['station', 'location', 'channel'] and 'network' not in params:
                    raise NoNetwork
                else:
                    param_value_dict[key] = params.get(key)
        # parameters that can have multiple values
        else:
            # if user is not a data center operator only one network can be specified at a time
            if key == 'network' and one_network:
                log.debug('Network: '+params.get(key))
                param_value_dict[key] = [params.get(key)]
            # if /raw method is used and user is not a data center operator and one of the below is true, then only one datacenter can be specified
            elif key == 'datacenter' and one_network and 'raw' in request.url:
                log.debug('Datacenter: '+params.get(key))
                param_value_dict[key] = [params.get(key)]
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
            # check if datacenter exists
            elif key == 'datacenter':
                try:
                    acceptable_nodes = get_nodes(request, internalCall=True).json['nodes']
                except Exception as e:
                    raise Exception(e)
                log.info('Got available datacenters from database')
                if any(x not in acceptable_nodes for x in param_value_dict[key]):
                    raise ValueError(key)
            # details parameter special handling
            elif key == 'details':
                if any(x not in ['month', 'year', 'country'] for x in param_value_dict[key]) or all(x in param_value_dict[key] for x in ['month', 'year']):
                    raise ValueError(key)

    log.debug('Final parameters: '+str(param_value_dict))
    return param_value_dict
