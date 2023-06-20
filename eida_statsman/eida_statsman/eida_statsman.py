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
from .model import Node, Token, Network

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

@click.command(name='update', help="Modify name or email")
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
    node.updated_at = datetime.now()
    if click.confirm(f"Update node {node}?"):
        ctx.obj['session'].commit()
    else:
        ctx.obj['session'].rollback()

@click.command(name='set_group', help="Define EAS group name authorized to see restricted statistics.")
@click.pass_context
@click.argument('nodeid', type=int)
@click.argument('eas_group')
def nodes_group(ctx, nodeid, eas_group):
    click.echo("Updating node eas_group")
    node = ctx.obj['session'].query(Node).filter(Node.id == nodeid).first()
    if node == None:
        click.echo(f"Node id {nodeid} not found")
        sys.exit(1)
    node.eas_group = eas_group
    ctx.obj['session'].commit()

@click.command(name='set_policy',help="""Change the policy of the node.
If set to 1/True all networks of node turn their inverted_policy to 0 to conform with restriction.
If policy is set to 0/False, all networks of node with inverted_policy = 1 are printed to inform user that they will become restricte."""
               )
@click.pass_context
@click.argument('nodeid', type=int)
@click.argument('policy', type=bool)
def nodes_policy(ctx, nodeid, policy):
    click.echo("Updating node default restriction policy")
    node = ctx.obj['session'].query(Node).filter(Node.id == nodeid).first()
    if node == None:
        click.echo(f"Node id {nodeid} not found")
        sys.exit(1)
    node.restriction_policy = policy
    if not policy:
        click.echo("The following networks will now be restricted:")
        for n in ctx.obj['session'].query(Network).join(Node).filter(Node.id == nodeid).filter(Network.inverted_policy == True):
            click.echo(n)
    else:
        click.echo(f"All networks of node id {nodeid} are now restricted")
        for n in ctx.obj['session'].query(Network).join(Node).filter(Node.id == nodeid).filter(Network.inverted_policy == True):
            n.inverted_policy = 0
    ctx.obj['session'].commit()


@click.group()
@click.pass_context
def networks(ctx):
    click.echo("Networks management")

@click.command(name='list')
@click.pass_context
@click.option('--node', '-n', help="Node id", type=int)
def networks_list(ctx, node):
    click.echo("Listing networks")
    if node:
        for n in ctx.obj['session'].query(Network).filter(Network.node_id == node):
            click.echo(n)
    else:
        for n in ctx.obj['session'].query(Network):
            click.echo(n)

@click.command(name='set_group')
@click.pass_context
@click.argument('nodeid', type=int)
@click.argument('netname')
@click.argument('eas_group')
def networks_group(ctx, nodeid, netname, eas_group):
    click.echo("Updating network eas_group")
    network = ctx.obj['session'].query(Network).join(Node).filter(Node.id == nodeid).filter(Network.name == netname).first()
    if network == None:
        click.echo(f"Network with name '{netname}' of node id {nodeid} not found")
        sys.exit(1)
    network.eas_group = eas_group
    ctx.obj['session'].commit()

@click.command(name='set_policy')
@click.pass_context
@click.argument('nodeid', type=int)
@click.argument('netname')
@click.argument('policy', type=bool)
def networks_policy(ctx, nodeid, netname, policy):
    click.echo("Updating network restriction policy status")
    network = ctx.obj['session'].query(Network).join(Node).filter(Node.id == nodeid).filter(Network.name == netname).first()
    if network == None:
        click.echo(f"Network with name '{netname}' of node id {nodeid} not found")
        sys.exit(1)
    network.inverted_policy = policy
    ctx.obj['session'].commit()


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
nodes.add_command(nodes_group)
nodes.add_command(nodes_policy)
networks.add_command(networks_list)
networks.add_command(networks_group)
networks.add_command(networks_policy)
tokens.add_command(tokens_list)
tokens.add_command(tokens_add)
tokens.add_command(tokens_rev)
tokens.add_command(tokens_mv)
cli.add_command(nodes)
cli.add_command(networks)
cli.add_command(tokens)

if __name__ == '__main__':
    cli()
