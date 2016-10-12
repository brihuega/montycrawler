from threading import Thread
import time
from urllib import request, error
import posixpath
from urllib.parse import urlparse
import datetime
import sys
from random import randint

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

# TODO finish thread gracefully on unexpected errors


class Dispatcher(Thread):
    """Thread based queue processor"""

    next_id = 0

    def __init__(self, queue, parser, processor,
                 logger, max_depth,
                 download_folder, rejected_folder,
                 min_relevancy):
        Thread.__init__(self, name=str(Dispatcher.next_id))
        Dispatcher.next_id += 1
        self.queue = queue
        self.parser = parser
        self.processor = processor
        self.logger = logger
        self.max_depth = max_depth
        self.download_folder = download_folder
        self.rejected_folder = rejected_folder
        self.min_relevancy = min_relevancy
        self.parsed = 0
        self.downloaded = 0
        self.added = 0
        self.start_time = None

    def run(self):
        self.start_time = time.time()
        self.logger.info('THREAD_STARTED')
        self.write_status('WAITING')
        # Random time between 3 and 7 seconds waiting to fill queue
        time.sleep(randint(3, 7))
        try:
            # Iterate until there're no other threads running
            waits = 0
            self.write_status('RUNNING')
            while self.logger.some_running():
                try:
                    item = next(self.queue)
                    self.write_status('RUNNING')
                    self.logger.info('PROCESS_URL', item.resource.url)
                    code, mimetype, filename, content, encoding = download(item.resource.url)
                    # Manage response
                    process_ok = False
                    if code:
                        item.resource.last_code = code
                        item.resource.fetched = datetime.datetime.utcnow()
                        if code == 200:
                            # Processing based on mime type
                            if mimetype == 'text/html':
                                # Limit depth in link search
                                if self.max_depth is None or item.depth < self.max_depth:
                                    # Decode content
                                    decoded = None
                                    if encoding:
                                        try:
                                            decoded = content.decode(encoding)
                                        except UnicodeDecodeError:
                                            self.logger.error('Decoding error: ' + item.resource.url)
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
                                        for link, text, priority in item_list:
                                            self.logger.debug('Found "%s" (p=%s) (%s)' %
                                                              (link,
                                                               'N' if priority is None else str(priority),
                                                               text[:40] if text is not None else ''))
                                        (a, r) = self.queue.add_list(item, title, item_list)
                                        self.added += a
                                        self.write_status('RUNNING')
                                        self.logger.debug('%d resources in queue. %d added and %d rejected from %s' %
                                                          (len(self.queue), a, r, item.resource.url))
                                        self.logger.console('Queue: %d resources. %d added from "%s".' %
                                                            (len(self.queue), a, item.resource.url))
                                        process_ok = True
                                    else:
                                        self.logger.error("Can't decode: " + item.resource.url)
                                else:
                                    self.logger.info('MAX_DEPTH_REACHED', item.resource.url)
                                    process_ok = True
                            elif mimetype == 'application/pdf':
                                # Process
                                (relevancy, metadata) = self.processor.process(content, mimetype)
                                # Store PDF
                                name = self.queue.store(relevancy >= self.min_relevancy,
                                                        item.resource, mimetype,
                                                        self.download_folder,
                                                        self.rejected_folder,
                                                        filename, metadata, content)
                                self.downloaded += 1
                                self.write_status('RUNNING')
                                self.logger.debug('Got document "%s" (relevancy=%d) from %s' %
                                                  (name, relevancy, item.resource.url))
                                self.logger.console('Document found (relevancy %.1f): %s' % (relevancy, name))
                                self.logger.info('DOWNLOADED', name)
                                process_ok = True
                            else:
                                self.logger.debug('Discarded type "%s" from %s' %
                                                  (mimetype, item.resource.url))
                        else:
                            self.logger.error('Got code %d retrieving %s' % (code, item.resource.url))
                    else:
                        self.logger.error('Unreachable: ' + item.resource.url)
                    # Remove processed item from queue or retry
                    if process_ok:
                        self.logger.info('PROCESSED_OK', item.resource.url)
                        self.queue.discard(item)
                        self.parsed += 1
                        self.write_status('RUNNING')
                    else:
                        self.logger.error("Can't retrieve: " + item.resource.url)
                        if self.queue.discard_or_retry(item):
                            self.logger.error('Reached maximum retries, discarded: ' + item.resource.url)
                except StopIteration:
                    self.write_status('WAITING')
                    waits += 1
                    self.logger.debug('Reached end of queue. %d waits.' % waits)
                    time.sleep(randint(3, 7))
        except (KeyboardInterrupt, SystemExit):
            self.write_status('INTERRUPTED')
            self.logger.debug('Thread interrupted.')
            raise
        except Exception as e:
            self.logger.error('Thread %s aborted by unexpected error: %s' % (self.name, e))
            self.logger.info('THREAD_ABORTED', self.name)
            self.write_status('ABORTED')
            raise

        self.write_status('FINISHED')
        self.logger.info('THREAD_FINISHED')

    def write_status(self, status):
        self.logger.status(status, self.parsed, self.added, self.downloaded, self.start_time)


def download(url):
    """Download URL content and obtain mime type"""
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
