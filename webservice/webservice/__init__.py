from pyramid.config import Configurator
import os


def main(global_config, **settings):
    config = Configurator(settings=settings)
    config.add_settings({'DBURI': os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')})
    config.add_route('health', '/_health')
    config.add_route('nodes', '/_nodes')
    config.add_route('dataselectstats', '/dataselect/stats')
    config.add_route('dataselectquery', '/dataselect/query')
    config.add_route('submitstat', '/submit')
    config.scan('.views')
    return config.make_wsgi_app()
