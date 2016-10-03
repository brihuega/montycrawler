from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

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


def setupdb(file, base, reset=False):
    """Helper procedure to connect session, create database and setup tables"""

    # Setup DB
    engine = create_engine('sqlite:///' + file + '.sqlite')
    if reset:
        base.metadata.drop_all(engine)
    base.metadata.create_all(engine)

    # Get DB session
    # We use scoped sessions for multithreading
    # see http://docs.sqlalchemy.org/en/latest/orm/contextual.html
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)
