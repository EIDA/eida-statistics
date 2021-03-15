#!/usr/bin/env python3
from sqlalchemy import Column, Sequence, String, Date, Integer, ForeignKey, BigInteger, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Node(Base):
    """
    EIDA node
    """
    __tablename__ = 'nodes'
    id = Column(Integer, Sequence('nodes_id_seq'), primary_key=True)
    name = Column(String(16))
    contact = Column(String())
    created_at = Column(DateTime())
    updated_at = Column(DateTime())
    tokens = relationship("Token", back_populates="node")

    def __repr__(self):
        return f"id: {self.id}, name: {self.name}, contact: {self.contact}, created_at: {self.created_at}, updated_at: {self.updated_at}"

class Token(Base):
    """
    AutBasehentication token
    """
    __tablename__ = 'tokens'
    id = Column(Integer, Sequence('tokens_id_seq'), primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    value = Column(String)
    valid_from = Column(DateTime())
    valid_until = Column(DateTime())
    created_at = Column(DateTime())
    updated_at = Column(DateTime())
    node = relationship("Node", back_populates="tokens")

    def __repr__(self):
        return f"id: {self.id}, node: {self.node.name}, value: {self.value}, valid_from: {self.valid_from}, valid_until: {self.valid_until}, created_at: {self.created_at}, updated_at: {self.updated_at}"


class Payload(Base):
    """
    Payloads objects
    """
    __tablename__ = 'payloads'
    id = Column(Integer, Sequence('payloads_id_seq'), primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    stats_hash = Column(BigInteger) # For storing mmh3 hash
    created_at = Column(DateTime())
