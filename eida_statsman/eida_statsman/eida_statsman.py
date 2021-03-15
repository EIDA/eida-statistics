#!/usr/bin/env python3

import sys
import logging
import click
from datetime import date, datetime
import string
import random
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
@click.option('--name', '-n', required=True, help="Node name")
@click.option('--contact', '-c', required=True, help="Node's contact email")
def nodes_add(ctx, name, contact):
    click.echo("Adding nodes")
    ctx.obj['session'].add(Node(name=name, contact=contact))
    ctx.obj['session'].commit()


@click.command(name='del')
@click.argument('nodeids', nargs=-1, type=int)
@click.pass_context
def nodes_del(ctx, nodeids):
    if click.confirm(f"Delete nodes {nodeids}?"):
        for node_id in nodeids:
            ctx.obj['session'].query(Node).filter(Node.id == node_id).delete()
        ctx.obj['session'].commit()

@click.command(name='update')
@click.option('--name', '-n', help="New name")
@click.option('--contact', '-c', help="New contact email")
@click.argument('nodeid', nargs=1, type=int)
@click.pass_context
def nodes_update(ctx, nodeid, name, contact):
    node = ctx.obj['session'].query(Node).filter(Node.id == nodeid).first()
    if node == None:
        click.echo(f"Node id {nodeid} not found")
        sys.exit(1)
    if name:
        node.name = name
    if contact:
        node.contact = contact
    if click.confirm(f"Update node {node}?"):
        ctx.obj['session'].commit()
    else:
        ctx.obj['session'].rollback()


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
@click.option('--nodename', '-n', required=True, help="Node name")
@click.pass_context
def tokens_add(ctx, nodename):
    node = ctx.obj['session'].query(Node).filter(Node.name == nodename).first()
    if node == None:
        click.echo(f"Node {nodename} not found")
        sys.exit(1)
    token_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(20))
    click.echo(f"Adding token {token_string} to node {node}")
    ctx.obj['session'].add(Token(value=token_string, node=node, valid_from=date.today(), valid_until=date(2050,6,6)))
    ctx.obj['session'].commit()

@click.command(name='revoke')
@click.argument('ids', nargs=-1, type=int)
@click.pass_context
def tokens_rev(ctx, ids):
    if click.confirm(f"Revoke tokens {ids}?"):
        for i in ids:
            token = ctx.obj['session'].query(Token).filter(Token.id == i).first()
            token.valid_until=datetime.now()
            token.updated_at=token.valid_until
        ctx.obj['session'].commit()

@click.command(name='mv')
@click.pass_context
def tokens_mv():
    click.echo("Updating tokens")


nodes.add_command(nodes_list)
nodes.add_command(nodes_add)
nodes.add_command(nodes_del)
nodes.add_command(nodes_update)
tokens.add_command(tokens_list)
tokens.add_command(tokens_add)
tokens.add_command(tokens_rev)
tokens.add_command(tokens_mv)
cli.add_command(nodes)
cli.add_command(tokens)
