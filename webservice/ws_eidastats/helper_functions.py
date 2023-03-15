from datetime import datetime
import gnupg
import re
import os
import logging

log = logging.getLogger(__name__)

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
