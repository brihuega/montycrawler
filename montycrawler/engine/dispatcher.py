from threading import Thread
import time
from urllib import request, error
import posixpath
from urllib.parse import urlparse
import datetime
import sys

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

    next_id = 0

    def __init__(self, queue, parser, logger):
        Thread.__init__(self)
        self.id = Dispatcher.next_id
        Dispatcher.next_id += 1
        self.queue = queue
        self.parser = parser
        self.logger = logger
        self.downloaded = 0
        self.stored = 0
        self.start_time = None

    def run(self):
        self.start_time = time.time()
        try:
            # Iterate forever until end of queue on exception
            while True:
                item = next(self.queue)
                self.logger.log('[%d] Processing: %s' % (self.id, item.resource.url))
                code, mimetype, filename, content, encoding = download(item.resource.url)
                # Manage response
                process_ok = False
                if code:
                    item.resource.last_code = code
                    item.resource.fetched = datetime.datetime.utcnow()
                    if code == 200:
                        self.downloaded += 1
                        # Processing based on mime type
                        if mimetype == 'text/html':
                            # Decode content
                            decoded = None
                            if encoding:
                                try:
                                    decoded = content.decode(encoding)
                                except UnicodeDecodeError:
                                    self.logger.info('[%d] Unexpected decoding error: %s' % (self.id, item.resource.url))
                            else:
                                # Encoding not provided. Guess it.
                                guess = ['iso-8859-1', 'utf-8', 'windows-1251',
                                         'windows-1252', 'iso-8859-15', 'iso-8859-9', 'ascii']
                                for enc in guess:
                                    try:
                                        decoded = content.decode(enc)
                                        break
                                    except UnicodeDecodeError:
                                        pass
                            if decoded:
                                # Parse and add found resources
                                (title, item_list) = self.parser.parse(decoded)
                                self.logger.info('Links parsed:')
                                for link, text, priority in item_list:
                                    self.logger.info('%s (p=%s) (%s)' %
                                                     (link,
                                                      'N' if priority is None else str(priority),
                                                      text[:40] if text is not None else ''))
                                (a, r) = self.queue.add_list(item.resource, title, item_list)
                                self.logger.log('[%d] %d resources in queue. %d added and %d rejected from %s' %
                                                (self.id, len(self.queue), a, r, item.resource.url))
                                process_ok = True
                            else:
                                self.logger.info("[%d] Can't decode content: %s" % (self.id, item.resource.url))
                        elif mimetype == 'application/pdf':
                            # Store PDF
                            name = self.queue.store(item.resource, mimetype, filename, content)
                            self.stored += 1
                            self.logger.log('[%d] Added document "%s" from %s' % (self.id, name, item.resource.url))
                            process_ok = True
                        else:
                            self.logger.info('[%d] Discarded type "%s" from %s' % (self.id, mimetype, item.resource.url))
                    else:
                        print('[%d] Got code %d retrieving %s' % (self.id, code, item.resource.url), file=sys.stderr)
                else:
                    print("[%d] Unreachable: %s" % (self.id, item.resource.url), file=sys.stderr)
                # Remove processed item from queue or retry
                if process_ok:
                    self.logger.info('[%d] Process OK: %s' % (self.id, item.resource.url))
                    self.queue.discard(item)
                else:
                    self.logger.info("[%d] Can't retrieve: %s" % (self.id, item.resource.url))
                    if self.queue.discard_or_retry(item):
                        self.logger.info('[%d] Reached maximum retries, discarded.' % self.id)
        except StopIteration:
            pass
        self.logger.log('Closed dispatcher #%d with %d resources downloaded and %d documents stored.' % (self.id, self.downloaded, self.stored))
        self.logger.log('Total process time: %d seconds.' % round(time.time() - self.start_time, 2))


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
        content = response.read()
        return code, mimetype, filename, content, encoding
    except error.HTTPError as ex:
        print('Code %d retrieving %s' % (ex.code, url), file=sys.stderr)
        return ex.code, None, None, None, None
    except error.URLError as ex:
        print('Error retrieving "%s"' % url, file=sys.stderr)
        print(ex.reason, file=sys.stderr)
        return None, None, None, None, None
    finally:
        if response:
            response.close()
