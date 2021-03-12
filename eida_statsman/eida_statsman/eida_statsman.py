#!/usr/bin/env python3

import sys
import logging
import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from eida_statsman.model import Node, Token

logging.basicConfig(level=logging.INFO)

@click.group()
@click.pass_context
@click.option('--dburi', '-d', help="Database URI postgres://user:password@dbhost:dbport/dbname", envvar='DBURI', show_envvar=True)
@click.option('--noop', '-n', help="Pretend to do something, but don't", is_flag=True)
@click.option('--debug', '-v', help="Verbose output", is_flag=True)
def cli(ctx, dburi, noop, debug):
    ctx.ensure_object(dict)
    ctx.obj['noop'] = dburi
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    engine = create_engine(dburi)
    Session = sessionmaker(bind=engine)
    ctx.obj['session'] = Session()

@click.group()
@click.pass_context
def nodes(ctx):
    click.echo("Nodes management")

@click.command(name='list')
@click.pass_context
def nodes_list(ctx):
    click.echo("Listing nodes")
    for n in ctx.obj['session'].query(Node):
        click.echo(n)


@click.command(name='add')
@click.pass_context
@click.option('--name', '-n', help="Node name")
@click.option('--contact', '-c', help="Node's contact email")
def nodes_add(ctx, name, contact):
    click.echo("Adding nodes")
    ctx.obj['session'].add(Node(name=name, contact=contact))
    ctx.obj['session'].commit()



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
def tokens(ctx):
    click.echo("Tokens management")

@click.command(name='list')
@click.pass_context
def tokens_list(ctx):
    click.echo("Listing tokens")
    for t in ctx.obj['session'].query(Token):
        click.echo(t)

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
