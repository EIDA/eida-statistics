#!/usr/bin/env python3

from sqlalchemy import Column, Sequence, String, Date, Integer, ForeignKey, BigInteger, DateTime, Boolean, ForeignKeyConstraint
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import func

Base = declarative_base()

class Node(Base):
    """
    EIDA node
    """

    __tablename__ = 'nodes'
    id = Column(Integer, Sequence('nodes_id_seq'), primary_key=True)
    name = Column(String(16))
    contact = Column(String())
    created_at = Column(DateTime(), server_default=func.now())
    updated_at = Column(DateTime())
    restriction_policy = Column(Boolean)
    eas_group = Column(String(20))
    stats = relationship("DataselectStat", back_populates="node")
    nets = relationship("Network", back_populates="node")

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class DataselectStat(Base):
    """
    EIDA statistic
    """

    __tablename__ = 'dataselect_stats'
    node_id = Column(Integer, ForeignKey('nodes.id'), primary_key=True)
    date = Column(Date(), primary_key=True)
    network = Column(String(6), primary_key=True)
    station = Column(String(5), primary_key=True)
    location = Column(String(2), primary_key=True)
    channel = Column(String(3), primary_key=True)
    country = Column(String(2), primary_key=True)
    bytes = Column(BigInteger)
    nb_reqs = Column(Integer)
    nb_successful_reqs = Column(Integer)
    nb_failed_reqs = Column(Integer)
    clients = Column(String())
    created_at = Column(DateTime(), server_default=func.now())
    updated_at = Column(DateTime())
    node = relationship("Node", back_populates="stats")
    net = relationship("Network", foreign_keys=[node_id, network], back_populates="stats", overlaps="node,stats")
    __table_args__ = (ForeignKeyConstraint([node_id, network],
                                           ["networks.node_id", "networks.name"]), {})

    def to_dict(self):
        return {'month': str(self.date)[:-3], 'datacenter': '', 'network': self.network, 'station': self.station, 'location': self.location, 'channel': self.channel,
        'country': self.country, 'bytes': int(self.bytes), 'nb_reqs': self.nb_reqs, 'nb_successful_reqs': self.nb_successful_reqs, 'clients': self.clients}

    def to_dict_for_human(self):
        return {'date': '', 'datacenter': '', 'network': '', 'station': '', 'location': '', 'channel': '',
        'country': '', 'bytes': int(self.bytes), 'nb_reqs': self.nb_reqs, 'nb_successful_reqs': self.nb_successful_reqs, 'clients': int(self.clients)}


class Network(Base):
    """
    EIDA networks
    """

    __tablename__ = 'networks'
    node_id = Column(Integer, ForeignKey('nodes.id'), primary_key=True)
    name = Column(String(6), primary_key=True)
    inverted_policy = Column(Boolean)
    eas_group = Column(String(20))
    node = relationship("Node", back_populates="nets")
    stats = relationship("DataselectStat", back_populates="net", overlaps="node,stats")

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
