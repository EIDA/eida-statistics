from pyramid.config import Configurator
import os
import tempfile
import yaml

def prefix_openapi_spec(path, prefix=""):
    """
    Returns the openapi specification as a string
    Add prefix to all paths
    """
    print(f"Setting prefix {prefix}")
    with open(path,'r') as openapi_file:
        openapi_dict = yaml.safe_load(openapi_file)
        openapi_dict['paths'] = { prefix+k:v for k,v in openapi_dict['paths'].items() }
    return yaml.dump(openapi_dict)

def main(global_config, **settings):
    # Setting the prefix in the openapi spec
    prefix = os.getenv('EIDASTATS_API_PATH','')
    proto = os.getenv('EIDASTATS_API_PROTO','http')
    try:
        port = int(os.getenv('EIDASTATS_API_PORT', 80))
    except ValueError as e:
        print(e)
        return False

    openapi_doc = tempfile.NamedTemporaryFile(delete=False)
    prefixed_openapi_spec = prefix_openapi_spec(os.path.join(os.path.dirname(__file__), "openapi.yaml"), prefix)
    openapi_doc.write(prefixed_openapi_spec.encode("utf8"))

    config = Configurator(settings=settings)
    config.include("pyramid_openapi3")
    config.pyramid_openapi3_spec(openapi_doc.name, route=os.path.join(prefix, 'openapi.yaml'))
    config.pyramid_openapi3_add_explorer(route=prefix+"/", proto_port=(proto, port))
    config.registry.settings["pyramid_openapi3.enable_request_validation"] = False
    config.registry.settings["pyramid_openapi3.enable_response_validation"] = False
    config.add_route('health', prefix+'/_health')
    config.add_route('nodes', prefix+'/nodes')
    config.add_route('dataselectraw', prefix+'/dataselect/raw')
    config.add_route('dataselectrestricted', prefix+'/dataselect/restricted')
    config.add_route('dataselectpublic', prefix+'/dataselect/public')
    config.add_route('submitstat', prefix+'/submit')
    config.add_route('isrestricted', prefix+'/_isRestricted')
    config.add_route('noderestriction', prefix+'/node_restriction_policy')
    config.add_route('networkrestriction', prefix+'/network_restriction_policy')
    config.scan('.views_main')
    config.scan('.views_restrictions')
    config.scan('.views_submit')
    config.scan('.helper_functions')
    return config.make_wsgi_app()
