from urllib.parse import urljoin, urldefrag, urlparse
from db.model import Pending, Base, Resource, Link, Document
from db.utils import setupdb
from threading import RLock
import mimetypes
import os
import json

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

# TODO: queue stats


class Queue:
    """Manages the pending queue as an iterator.

    Retrieves the pending items from the database and manages the queue
    inserting and discarding them as requested by the dispatchers.

    Note:
        This class manages all database operations, including the initial setup.
        All operations have been isolated in one instance in order to avoid
        concurrent access to database (as SQLite doesn't support concurrency).
        It uses the `scoped session` paradigm of the SQLAlchemy engine, so each
        thread has its own session. An extra lock mechanism has been added to
        avoid collisions.

    """
    def __init__(self, reset=False, all_domains=False, retries=3):
        """Class initialization.

        Args:
            reset: T/F wipe database before start.
            all_domains: T = retrieve resources from any domain. F = only from origin domain.
            retries: Number of times to retry before discarding a resource as unreachable.
        """
        self.all_domains = all_domains
        self.retries = retries
        self.lock = RLock()
        self.session = setupdb('db', Base, reset)

        # Get current queue from database.
        # The session is scoped, for multithreading,
        # so we have to instantiate it before each use
        #
        # Order by the "priority" or "id" columns.
        # Using fake order clause because SQLite doesn't support NULLS LAST
        q = self.session().query(Pending).order_by(Pending.priority == None, Pending.priority.desc(), Pending.id).all()
        # Cache queue IDs and priorities to avoid repeating access to DB
        self.queue = [(item.id, item.priority) for item in q]
        # Cache URL resources
        resources = self.session().query(Resource).all()
        self.urlcache = [res.url for res in resources]

    def __len__(self):
        """Magic method for len()
        Returns:
            Length of the queue
        """
        return len(self.queue)

    def __iter__(self):
        """Magic method for iter()
        Returns:
            Iterable instance of the queue (self)
        """
        return self

    def __next__(self):
        """Magic method for next()
        Returns:
            Next item in the queue (`Pending` instance).
        """
        # Make queue operation atomic
        with self.lock:
            if self.queue:
                # Pop element from top of the cached list
                # (on database isn't removed until call to discard or discard_or_retry)
                i, _ = self.queue.pop(0)
                # Obtain object from DB by ID
                return self.session().query(Pending).filter_by(id=i).one()
            else:
                raise StopIteration

    def insert(self, item):
        """Inserts an item in the queue.
        Args:
            item: `Pending` item.
        """
        """Ordered insert element on the cached queue"""
        # Items are tuples of (id, priority)
        i, p = item
        # Protect queue reshape from concurrency
        with self.lock:
            if p is None:
                # No priority, append to end
                self.queue.append(item)
            else:
                newqueue = []
                inserted = False
                for item1 in self.queue:
                    i1, p1 = item1
                    # Insert item when lower priority found
                    if not inserted and \
                            (p1 is None or p1 < p):
                        newqueue.append(item)
                        inserted = True
                    # Copy items except same id
                    if i1 != i:
                        newqueue.append(item1)
                if not inserted:
                    newqueue.append(item)
                self.queue = newqueue

    def add(self, resource, referrer=None, priority=None):
        """
        Add resource to queue and database (if not exists)

        Args:
            resource: The resource to be added
            referrer: The referrer pending item in queue
            priority: Integer to set order in the queue

        Returns:
            Tuple:
                Pending item.
                The item is new in queue (boolean).

        Raises:
            UrlNotValidError: URL not HTTP or HTTPS or host component empty.
        """
        # Normalize and complete URL
        norm, _ = urldefrag(resource.url)
        if referrer:
            norm = urljoin(referrer.resource.url, norm)
        ind = norm.find(';jsessionid')
        if ind != -1:
            norm = norm[0:ind]
        # Only valid protocols
        parsed = urlparse(norm)
        if parsed.scheme not in ('http', 'https') or not parsed.netloc:
            raise MalformedUrlError(
                'URL "%s" not valid. Use HTTP or HTTPS with at least the host component.' % norm)
        # By default limit to the same base domain
        if not self.all_domains and referrer and parsed.netloc != urlparse(referrer.resource.url).netloc:
            raise NotInBaseDomainError('URL "%s" not in the base domain.' % norm)
        resource.url = norm
        # Look if resource on queue
        with self.lock:
            if resource.url in self.urlcache:
                existing = self.session().query(Pending).join(Resource).filter(Resource.url == resource.url)
                if existing.count() == 0:
                    # Append operation must be protected from concurrency
                    # Look if resource already exists
                    actual = self.session().query(Resource).filter_by(url=resource.url).first()
                    # Add pending item with priority and increase depth
                    new = Pending(resource=actual, priority=priority,
                                  depth=referrer.depth + 1 if referrer is not None else 0)
                    # Add item to queue
                    self.session().add(new)
                    self.session().commit()
                    self.insert((new.id, priority))
                    return new, True
                else:
                    old = existing.first()
                    # Override priority if bigger
                    if priority is not None and \
                            (old.priority is None or priority > old.priority):
                        old.priority = priority
                        self.session().commit()
                        self.insert((old.id, priority))
                    return old, False
            else:
                self.session().add(resource)
                new = Pending(resource=resource, priority=priority,
                              depth=referrer.depth + 1 if referrer is not None else 0)
                self.session().add(new)
                self.session().commit()
                self.insert((new.id, priority))
                self.urlcache.append(resource.url)
                return new, True

    def add_list(self, ref, title, links):
        """Adds resources to the queue from a list of links.
        Args:
            ref: Referrer resource.
            title: Referrer title.
            links: List of links (tuples):
                URL
                Title
                Priority
        Returns:
            Number of items added and rejected (tuple).
        """
        added = 0
        rejected = 0
        if title:
            ref.resource.title = title
            self.session().commit()
        for u, t, p in links:
            try:
                with self.lock:
                    # Add url to queue
                    (p, new) = self.add(Resource(url=u, title=t), referrer=ref, priority=p)
                    # Create link
                    self.session().add(Link(text=t, referrer=ref.resource, target=p.resource))
                    self.session().commit()
                if new:
                    added += 1
            except UrlNotValidError:
                rejected += 1
        return added, rejected

    def discard_or_retry(self, item):
        """If a failed item has reached its maximum retries, discard it. It not, increase
        retry count, decrease to half priority and re-insert it in the queue.

        Args:
            item: The item
        Returns:
            T/F the item was deleted.
        """
        with self.lock:
            if item.retries + 1 >= self.retries:
                self.session().delete(item)
                self.session().commit()
                return True
            else:
                # Increase retries and reduce half priority
                if item.priority is not None:
                    item.priority //= 2
                item.retries += 1
                self.session().commit()
                # Insert in new place
                self.insert((item.id, item.priority))
                return False

    def discard(self, item):
        """Remove resource from pending items in database (if exists).
        Args:
            item: The item to be removed.
        """
        with self.lock:
            self.session().delete(item)
            self.session().commit()

    def clear(self):
        """Empty the queue and delete all records"""
        with self.lock:
            n = len(self.queue)
            self.queue = []
            # Empty Pending table
            self.session().query(Pending).delete()
            self.session().commit()
        return n

    def store(self, accepted, resource, mimetype, folder, rejected_folder, filename, metadata, content):
        """Stores a document on filesystem and creates a new `Document` instance on database.
        Args:
            accepted: T/F write the document in the `accepted` folder, otherwise on `rejected`.
            resource: The URL resource where the document was retrieved from.
            mimetype: Standard MIME id for the type of content.
            folder: Path to the `accepted` folder.
            rejected_folder: Path to the `rejected` folder (content discarded if path is empty).
            filename: Name for the file (it will be cleaned from not allowed chars).
            metadata: Metadata dictionary for the document.
            content: The binary content to be stored.
        Returns:
            The final name of the file (even it was written or not).
        """
        # Clean filename
        cleaned = ''.join((c if c.isalnum() or c == '.' else '_' for c in filename))
        # Append extension
        ext = mimetypes.guess_extension(mimetype)
        if not cleaned.endswith(ext):
            cleaned += ext
        # Append ID to avoid collision
        cleaned = str(resource.id) + '_' + cleaned
        # Write to filesystem
        if accepted or rejected_folder:
            # Check binary or text mode
            mode = 'w' if mimetype.startswith('text/') else 'wb'
            path = os.path.join(folder if accepted else rejected_folder, cleaned)
            with open(path, mode) as f:
                f.write(content)
        # Register on db
        with self.lock:
            doc = Document(name=resource.title if metadata.get('/Title') is None else metadata.get('/Title'),
                           author=metadata.get('/Author'),
                           meta_data=json.dumps(metadata),
                           filename=cleaned,
                           type=mimetype,
                           relevancy=metadata.get('_relevancy'),
                           num_pages=metadata.get('_num_pages'),
                           accepted=accepted)
            self.session().add(doc)
            resource.document = doc
            self.session().commit()
        return cleaned


class UrlNotValidError(ValueError):
    """Base exception raised when the URL provided is invalid for program's purpose"""
    pass


class MalformedUrlError(UrlNotValidError):
    """The URL is malformed or protocol is not supported."""
    pass


class NotInBaseDomainError(UrlNotValidError):
    """The URL provided isn't from the same base domain."""
    pass
