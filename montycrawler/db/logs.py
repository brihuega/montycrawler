import enum
import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, Column, String, DateTime

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


class Message(Base):
    __tablename__ = 'messages'
    label = Column(String(25), primary_key=True)


class LogEntry(Base):
    __tablename__ = 'log_entries'
    id = Column(Integer, primary_key=True)
    type = Column(String(5))
    message_label = Column(String(25))
    text = Column(String(250))
    thread = Column(String(6))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)


class ThreadStatus(Base):
    __tablename__ = 'thread_status'
    id = Column(Integer, primary_key=True)
    thread = Column(String(6), unique=True)
    status = Column(String(10))
    running_time = Column(Integer())
    parsed = Column(Integer)
    added = Column(Integer)
    downloaded = Column(Integer)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)



