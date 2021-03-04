#!/usr/bin/env python3
from sqlalchemy import Column, Sequence, String, Date, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class node(Base):
    """
    EIDA node
    """
    __tablename__ = 'nodes'
    id = Column(Sequence, primary_key=True)
    name = Column(String(16))

class token(Base):
    """
    AutBasehentication token
    """
    __tablename__ = 'tokens'
    id = Column(Sequence, primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))

class payload(Base):
    """
    Payloads objects
    """
    __tablename__ = 'payloads'
    id = Column(Sequence, primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))

class dataselectstat(Base):
    """
    Aggregated dataselect statistic
    """
    __tablename__ = 'dataselectstats'
    date = Column(Date)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    network = Column(String(6))
    station = Column(String(6))
    location = Column(String(2))
    channel = Column(String(3))
    country = Column(String(2))
    # TODO client is HLL object
