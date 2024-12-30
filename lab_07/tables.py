from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

class Client(Base):
    __tablename__ = 'client'
    __table_args__ = {'schema': 't'}

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    surname = Column(String, nullable=False)
    birthday = Column(Date, nullable=False)
    address = Column(String, nullable=False)
    email = Column(String, nullable=False)
    passport = Column(String, nullable=False)

    def __str__(self):
        return (f"<Client(id={self.id}, name='{self.name}', surname='{self.surname}', "
                f"birthday='{self.birthday}', address='{self.address}', "
                f"email='{self.email}', passport='{self.passport}')>")

class Transport(Base):
    __tablename__ = 'transport'
    __table_args__ = {'schema': 't'}

    flight = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    surname = Column(String, nullable=False)
    mode = Column(String, nullable=False)
    company = Column(String, nullable=False)
    host = Column(String, nullable=False)
    cost = Column(Integer, nullable=False)
    CheckConstraint('cost >= 0')

class Trip(Base):
    __tablename__ = 'trip'
    __table_args__ = {'schema': 't'}

    trip_number = Column(Integer, primary_key=True)
    derapture = Column(String, nullable=False)
    arrival = Column(String, nullable=False)
    target = Column(String, nullable=False)
    number_people = Column(Integer, nullable=False)
    company = Column(String, nullable=False)
    flight_num = Column(Integer, ForeignKey('t.transport.flight'))
    CheckConstraint('number_people > 0')

    transport = relationship("Transport")

class LinksCT(Base):
    __tablename__ = 'links_ct'
    __table_args__ = {'schema': 't'}

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey('t.client.id'))
    trip_number = Column(Integer, ForeignKey('t.trip.trip_number'))
    number_seats = Column(Integer, nullable=False)
    CheckConstraint('number_seats > 0')

    client = relationship("Client")
    trip = relationship("Trip")
