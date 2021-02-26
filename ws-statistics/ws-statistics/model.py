#!/usr/bin/env python3
from sqlalchemy import Column, Sequence, String, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class node(Base):
    """
    EIDA node
    """
    id = Column(Sequence, primary_key=True)
    name = Column(String(16))

class token(Base):
    """
    AutBasehentication token
    """
    id = Column(Sequence, primary_key=True)

class payload(Base):
    """
    Payloads objects
    """
    id = Column(Sequence, primary_key=True)

class dataselectstat(Base):
    """
    Aggregated dataselect statistic
    """
    date = Column(Date)
