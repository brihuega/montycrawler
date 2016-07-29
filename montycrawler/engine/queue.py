from urllib.parse import urljoin, urldefrag, urlparse
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from db.model import Pending, Base, Resource, Link, Document
from threading import RLock
import mimetypes

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
	

class Queue:
    """Manages the pending queue as an iterator"""
    def __init__(self, reset=False):
        self.lock = RLock()
        self.session = setupdb(reset)
        # Order by the "order" or "id" columns.
        # Using fake order clause because SQLite doesn't support NULLS LAST
        self.pos = 0
        # Get current queue from database.
        # The session is scoped, so we have to instantiate it before each use
        # for multithreading
        # We use a weird order_by clause to prioritize the order field
        q = self.session().query(Pending).order_by(Pending.order == None, Pending.order, Pending.id).all()
        # Cache queue IDs to avoid repeating access to DB
        self.queue = [item.id for item in q]
        # Cache URL resources
        resources = self.session().query(Resource).all()
        self.urlcache = [res.url for res in resources]

    def __len__(self):
        return len(self.queue)

    def __iter__(self):
        self.pos = 0
        return self

    def __next__(self):
        # Make pointer operation atomic
        with self.lock:
            if self.pos < len(self.queue):
                self.pos += 1
                # Obtain object from DB by ID
                return self.session().query(Pending).filter_by(id=self.queue[self.pos - 1]).one()
            else:
                raise StopIteration

    def add(self, resource, referrer=None):
        """
        Add resource to queue and database (if not exists)

        Args:
            resource: The resource to be added
            referrer: The referrer resource

        Returns:
            Tuple:
                Pending item.
                The item already exist in queue (boolean).

        Raises:
            UrlNotValidError: URL not HTTP or HTTPS or host component empty.
        """
        # Normalize and complete URL
        norm, _ = urldefrag(resource.url)
        if referrer:
            norm = urljoin(referrer.url, norm)
        ind = norm.find(';jsessionid')
        if ind != -1:
            norm = norm[0:ind]
        # Only valid protocols
        parsed = urlparse(norm)
        if parsed.scheme not in ('http', 'https') or not parsed.netloc:
            raise UrlNotValidError(
                'URL "%s" not valid. Use HTTP or HTTPS with at least the host component.' % norm)
        resource.url = norm
        # Look if resource on queue
        with self.lock:
            if resource.url in self.urlcache:
                existing = self.session().query(Pending).join(Resource).filter(Resource.url == resource.url)
                if existing.count() == 0:
                    # Append operation must be protected from concurrency
                    # Look if resource already exists
                    actual = self.session().query(Resource).filter_by(url=resource.url).first()
                    new = Pending(resource=actual)
                    # Add item to queue
                    self.session().add(new)
                    self.session().commit()
                    self.queue.append(new.id)
                    return new, True
                else:
                    old = existing.first()
                    return old, False
            else:
                self.session().add(resource)
                new = Pending(resource=resource)
                self.session().commit()
                self.queue.append(new.id)
                self.urlcache.append(resource.url)
                return new, True

    def add_list(self, ref, title, links):
        """Adds resources to the queue from a list of links"""
        # TODO option to get URLs only from specific domain
        added = 0
        rejected = 0
        if title:
            ref.title = title
            self.session().commit()
        for u, t in links:
            try:
                # Add url to queue
                (p, new) = self.add(Resource(url=u, title=t), referrer=ref)
                # Create link
                self.session().add(Link(text=t, referrer=ref, target=p.resource))
                self.session().commit()
                if new:
                    added += 1
            except UrlNotValidError:
                rejected += 1
        return added, rejected

    def remove(self, item):
        """Remove resource from queue and database (if exists)"""
        # Avoid concurrency on queue reshaping
        with self.lock:
            if len(self.queue) > 0:
                # Build new queue removing ID of item
                # and adjust pointer
                newqueue = []
                i = 0
                for p in self.queue:
                    if p == item.id:
                        if i < self.pos:
                            self.pos -= 1
                    else:
                        newqueue.append(p)
                        i += 1
                self.queue = newqueue
                # Delete item from table
                self.session().delete(item)
                self.session().commit()

    def clear(self):
        """Empty the queue and delete all records"""
        with self.lock:
            n = len(self.queue)
            self.queue = []
            self.pos = 0
            # Empty Pending table
            self.session().query(Pending).delete()
            self.session().commit()
        return n

    def store(self, resource, mimetype, filename, content):
        """Stores document on filesystem"""
        # Clean filename
        cleaned = ''.join((c if c.isalnum() or c == '.' else '_' for c in filename))
        # Append extension
        ext = mimetypes.guess_extension(mimetype)
        if not cleaned.endswith(ext):
            cleaned += ext
        # Append ID to avoid collision
        cleaned = str(resource.id) + '_' + cleaned
        # Write to filesystem
        # Check binary or text mode
        mode = 'w' if mimetype.startswith('text/') else 'wb'
        with open(cleaned, mode) as f:
            f.write(content)
        # Register on db
        doc = Document(name=resource.title, filename=cleaned, type=mimetype)
        self.session().add(doc)
        resource.document = doc
        self.session().commit()
        return cleaned


class UrlNotValidError(ValueError):
    """Exception raised when the URL provided is invalid for program's purpose"""
    pass


def setupdb(reset=False):
    """Helper procedure to connect session, create database and setup tables"""

    # Setup DB
    engine = create_engine('sqlite:///db.sqlite')
    if reset:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # Get DB session
    # We use scoped sessions for multithreading
    # see http://docs.sqlalchemy.org/en/latest/orm/contextual.html
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)
