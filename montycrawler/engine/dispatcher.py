from threading import Thread
import time
from urllib import request, error
import posixpath
from urllib.parse import urlparse
import datetime

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
	
class Dispatcher(Thread):
    """Thread based queue processor"""

    next_id = 1

    def __init__(self, queue, parser):
        Thread.__init__(self)
        self.id = Dispatcher.next_id
        Dispatcher.next_id += 1
        self.queue = queue
        self.parser = parser
        self.downloaded = 0
        self.stored = 0
        self.start_time = None

    def run(self):
        self.start_time = time.time()
        try:
            # Iterate forever until end of queue on exception
            while True:
                item = next(self.queue)
                print('[%d] Processing: %s' % (self.id, item.resource.url))
                code, mimetype, filename, content, encoding = download(item.resource.url)
                # Manage response
                if code:
                    item.resource.last_code = code
                    item.resource.fetched = datetime.datetime.utcnow()
                    if code == 200:
                        self.downloaded += 1
                        # Processing based on mime type
                        if mimetype == 'text/html':
                            # Parse and add found resources
                            (title, item_list) = self.parser.parse(content.decode(encoding))
                            print('Links parsed:')
                            for link, text in item_list:
                                print('%s (%s)' % (link, text[:15] if text is not None else ''))
                            (a, r) = self.queue.add_list(item.resource, title, item_list)
                            print('[%d] %d items added and %d rejected from %s' % (self.id, a, r, item.resource.url))
                        elif mimetype == 'application/pdf':
                            # Store PDF
                            name = self.queue.store(item.resource, mimetype, filename, content)
                            self.stored += 1
                            print('[%d] Added document "%s" from %s' % (self.id, name, item.resource.url))
                        else:
                            print('[%d] Discarded type "%s" from %s' % (self.id, mimetype, item.resource.url))
                    else:
                        print('[%d] Got code %d retrieving %s' % (self.id, code, item.resource.url))
                else:
                    print("[%d] Unreachable: %s" % (self.id, item.resource.url))
                # Remove processed item from queue
                # TODO preserve failed items and retry
                self.queue.remove(item)
        except StopIteration:
            pass
        print('Closed dispatcher #%d with %d items downloaded and %d stored.' % (self.id, self.downloaded, self.stored))
        print('Total process time: %d seconds.' % round(time.time() - self.start_time, 2))


def download(url):
    """Download URL content and obtain mime type"""
    code = None
    response = None
    try:
        response = request.urlopen(url)
        code = response.getcode()
        mimetype = response.info().get_content_type()
        filename = response.info().get_filename()
        if not filename:
            # Guess filename from URL
            filename = posixpath.basename(urlparse(url).path)
        encoding = response.info().get_content_charset()
        if not encoding:
            encoding = 'utf-8'
        content = response.read()
        return code, mimetype, filename, content, encoding
    except error.HTTPError as ex:
        print('Code %d retrieving %s' % (ex.code, url))
        return ex.code, None, None, None, None
    except error.URLError as ex:
        print('Error retrieving %s')
        print(ex.reason)
        return None, None, None, None, None
    finally:
        if response:
            response.close()
