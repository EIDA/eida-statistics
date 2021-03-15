#!/usr/bin/env python3

import sys
import logging
from datetime import date, datetime
import string
import random
import smtplib
from email.message import EmailMessage
import ssl
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
@click.option('--webservice-url', help="URL of the ingestor webservice", envvar="EIDASTATS_URL", show_envvar=True, default="https://ws.resif.fr/eidaws/statistics/1")
def cli(ctx, dburi, noop, debug, webservice_url):
    ctx.ensure_object(dict)
    ctx.obj['noop'] = noop
    ctx.obj['wsurl'] = webservice_url
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
@click.option('--smtp-server', '-s', help="STMP server to send email notification", envvar="SMTP_SRV", show_envvar=True)
@click.option('--smtp-user', '-u', help="SMTP login", envvar="SMTP_USER", show_envvar=True)
@click.option('--smtp-password', prompt=True, hide_input=True, envvar="SMTP_PASSWORD", show_envvar=True)
@click.pass_context
def tokens_add(ctx, nodename, smtp_server, smtp_user, smtp_password):
    node = ctx.obj['session'].query(Node).filter(Node.name == nodename).first()
    if node == None:
        click.echo(f"Node {nodename} not found")
        sys.exit(1)
    token_string = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(20))
    token = Token(value=token_string, node=node, valid_from=date.today(), valid_until=date(2050,6,6))
    click.echo(f"Adding token {token_string} to node {node}")
    if smtp_server and smtp_user:
        if click.confirm(f"Send notification to {node.contact}?"):
            # Send notification to contact email
            msg = EmailMessage()
            msg['Subject'] = f"[EIDA statistics management] A new token has been generated for {node.name}"
            msg['To'] = node.contact
            msg['From'] = 'eida statistics manager <noreply@resif.fr>'
            msg.set_content(f"""Your new token to submit statistics to the eida central stats service {ctx.obj['wsurl']} has been generated.

This token is valid from {token.valid_from} until {token.valid_until}.

The token value is {token.value}

Submit your statistics json files, either with curl :

    curl  --header "Authentication: Bearer {token.value}"  --header "Content-Type: application/json" --data-binary "@aggregation/2021-02-22_2021-02-22.json" {ctx.obj['wsurl']}

Or with the eida-statistics-aggregator https://pypi.org/project/eida-statistics-aggregator

    eida_stats_aggregator –output-directory aggregates fdsnws-requests.log.2020-11-02 --send-to  ctx.obj['wsurl'] --token {token.value}

If you need help or have questions, please submit an issue https://github.com/EIDA/etc/issues (you need to be logged in and member of EIDA/etc). """)
            context = ssl.create_default_context()
            try:
                with smtplib.SMTP_SSL(smtp_server, 465, context=context) as server:
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
            except Exception as err:
                click.echo(f"Error trying to send email to {smtp_server} as {smtp_user}")
                click.echo("No token generated")
                sys.exit(1)
    ctx.obj['session'].add(token)
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
