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


class NoDatacenter(Exception):
    "Raised when datacenter parameter must be specified"
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
        accepted += ['station', 'location', 'channel', 'aggregate_on', 'format']
    # parameters accepted by public method
    elif 'public' in request.url:
        accepted += ['aggregate_on', 'format']

    params = request.params
    # make start parameter mandatory
    if 'start' not in params:
        raise Mandatory
    # if user is not a data center operator and uses /raw method, then both datacenter and network parameters must be specified
    if 'raw' in request.url and one_network and any(x not in params for x in ['datacenter', 'network']):
        raise NoDatacenterAndNetwork
    # if user is not a data center operator and uses /restricted method and specifies any of network, station, channel, location parameters,
    # then both datacenter and network parameters must be specified
    if 'restricted' in request.url and one_network and any(x in params for x in ['network', 'station', 'channel', 'location'])\
    and any(x not in params for x in ['datacenter', 'network']):
        raise NoDatacenterAndNetwork
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
        else:
            # if user is not a data center operator only one network can be specified at a time
            if key == 'network' and one_network:
                log.debug('Network: '+params.get(key))
                param_value_dict[key] = [params.get(key)]
                # if user is not operator and network is specified, then datacenter must be also specified
                if 'datacenter' not in params:
                    raise NoDatacenter
            # if user is not a data center operator and one of the below is true, then only one datacenter can be specified:
            # - /raw method called
            # - network is specified
            # - /restricted method is called and any of country, channel, location parameters is specified
            elif key == 'datacenter' and one_network and ('raw' in request.url or 'network' in params or\
            ('restricted' in request.url and any(x in params for x in ['station', 'channel', 'location']))):
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
                # force station aggregation always and network aggregation when in data center level for public method
                if 'public' in request.url:
                    if 'station' not in param_value_dict[key]:
                        param_value_dict[key].append('station')
                    if 'network' not in params and 'network' not in param_value_dict[key]:
                        param_value_dict[key].append('network')
                # force network and station aggregation when in data center level for restricted method
                if 'restricted' in request.url and all(x not in params for x in ['network', 'station', 'channel', 'location']):
                    if 'station' not in param_value_dict[key]:
                        param_value_dict[key].append('station')
                    if 'network' not in param_value_dict[key]:
                        param_value_dict[key].append('network')

    # below lines needed in case aggregate_on or format parameters are not specified at all by the user in /public and /restricted methods
    # default parameters to be aggregated in /restricted and /public methods: location, channel
    if ('restricted' in request.url or 'public' in request.url) and 'aggregate_on' not in param_value_dict:
        param_value_dict['aggregate_on'] = ['location', 'channel']
        # force station aggregation always for /public method
        if 'public' in request.url:
            param_value_dict['aggregate_on'].append('station')
            # force network aggregation in data center level for /public method
            if 'network' not in param_value_dict:
                param_value_dict['aggregate_on'].append('network')
        # force network and station aggregation in data center level for /restricted method
        elif 'restricted' in request.url and all(x not in params for x in ['network', 'station', 'channel', 'location']):
            param_value_dict['aggregate_on'] += ['network', 'station']

    # default output format: csv
    if ('restricted' in request.url or 'public' in request.url) and 'format' not in param_value_dict:
        param_value_dict['format'] = 'csv'

    log.debug('Final parameters: '+str(param_value_dict))
    return param_value_dict
