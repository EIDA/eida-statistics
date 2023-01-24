from pyramid.config import Configurator
import os


def main(global_config, **settings):
    config = Configurator(settings=settings)
    config.add_settings({'DBURI': os.getenv('DBURI', 'postgresql://postgres:password@localhost:5432/eidastats')})
    config.add_route('hello', '/')
    config.add_route('health', '/_health')
    config.scan('.views')
    return config.make_wsgi_app()
