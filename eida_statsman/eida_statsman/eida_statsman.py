#!/usr/bin/env python3

import sys
import logging
import click
import psycopg2
logging.basicConfig(level=logging.INFO)


@click.group()
@click.pass_context
@click.option('--dburi', '-d', help="Database URI postgres://user:password@dbhost:dbport/dbname", envvar='DBURI', show_envvar=True)
@click.option('--noop', '-n', help="Pretend to do something, but don't", is_flag=True)
@click.option('--debug', '-v', help="Verbose output", is_flag=True)
def cli(ctx, dburi, noop, debug):
    ctx.ensure_object(dict)
    ctx.obj['dburi'] = dburi
    ctx.obj['noop'] = dburi
    if debug is not None:
        logging.getLogger().setLevel(logging.DEBUG)
    pass

@click.group()
@click.pass_context
def nodes(ctx):
    click.echo("Nodes management")
    logging.debug("Database URI: %s",ctx.obj['dburi'])

@click.command(name='list')
@click.pass_context
def nodes_list(ctx):
    click.echo("Listing nodes")

@click.command(name='add')
@click.pass_context
def nodes_add():
    click.echo("Adding nodes")

@click.command(name='del')
@click.pass_context
def nodes_del():
    click.echo("Deleting nodes")

@click.command(name='mv')
@click.pass_context
def nodes_mv():
    click.echo("Updating nodes")

@click.group()
@click.pass_context
def tokens():
    click.echo("Tokens management")

@click.command(name='list')
@click.pass_context
def tokens_list():
    click.echo("Listing tokens")

@click.command(name='add')
@click.pass_context
def tokens_add():
    click.echo("Adding tokens")

@click.command(name='del')
@click.pass_context
def tokens_del():
    click.echo("Deleting tokens")

@click.command(name='mv')
@click.pass_context
def tokens_mv():
    click.echo("Updating tokens")


nodes.add_command(nodes_list)
nodes.add_command(nodes_add)
nodes.add_command(nodes_del)
nodes.add_command(nodes_mv)
tokens.add_command(tokens_list)
tokens.add_command(tokens_add)
tokens.add_command(tokens_del)
tokens.add_command(tokens_mv)
cli.add_command(nodes)
cli.add_command(tokens)
