import uuid
import datetime
from sqlalchemy import Integer, Column, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

# Copyright 2016 Jose A. Brihuega Parodi <jose.brihuega@uca.es>

# This file is part of Montycrawler.

# Montycrawler is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Montycrawler is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Montycrawler.  If not, see <http://www.gnu.org/licenses/>.

Base = declarative_base()


class Document(Base):
    __tablename__ = 'documents'
    id = Column(Integer, primary_key=True)
    name = Column(Text())
    type = Column(String(60))
    filename = Column(String(255))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    uuid = Column(String(32), unique=True, default=lambda: str(uuid.uuid4()))


class Resource(Base):
    __tablename__ = 'resources'
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=True)
    url = Column(String(255), unique=True, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    fetched = Column(DateTime, nullable=True)
    last_code = Column(Integer, nullable=True)
    document_id = Column(Integer, ForeignKey('documents.id'), nullable=True)
    document = relationship('Document', backref=backref('resources', lazy='dynamic'))


class Link(Base):
    __tablename__ = 'links'
    id = Column(Integer, primary_key=True)
    text = Column(Text(), nullable=True)
    referrer_id = Column(Integer, ForeignKey('resources.id'))
    referrer = relationship('Resource', foreign_keys=[referrer_id],
                            backref=backref('links', lazy='dynamic'))
    target_id = Column(Integer, ForeignKey('resources.id'))
    target = relationship('Resource', foreign_keys=[target_id],
                          backref=backref('backlinks', lazy='dynamic'))


class Pending(Base):
    __tablename__ = 'pending'
    id = Column(Integer, primary_key=True)
    priority = Column(Integer, nullable=True)
    resource_id = Column(Integer, ForeignKey('resources.id'), unique=True)
    resource = relationship('Resource', backref='pending', uselist=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    retries = Column(Integer, default=0)
