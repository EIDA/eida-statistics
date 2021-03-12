#!/usr/bin/env python3

import click

@click.group()
def nodes():
    click.echo("Nodes management")

@nodes.command()
def list():
    click.echo("Listing nodes")

@nodes.command()
def add():
    click.echo("Adding nodes")

@nodes.command()
def delete():
    click.echo("Deleting nodes")

@nodes.command()
def mv():
    click.echo("Updating nodes")
