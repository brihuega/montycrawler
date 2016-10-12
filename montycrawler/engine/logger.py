from db.utils import setupdb
from db.logs import Base, LogEntry, Message, ThreadStatus
from threading import RLock, current_thread
import time

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


class Logger:
    """Generate and store logs in a separate detabase"""
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.session = setupdb('log', Base, True)
        self.lock = RLock()

        # Fill message labels
        messages = (
            Message(label='DEBUG'),
            Message(label='ERROR'),
            Message(label='PROCESS_URL'),
            Message(label='MAX_DEPTH_REACHED'),
            Message(label='PROCESSED_OK'),
            Message(label='THREAD_STARTED'),
            Message(label='THREAD_FINISHED'),
            Message(label='THREAD_ABORTED'),
            Message(label='DOWNLOADED'),
        )
        self.session().bulk_save_objects(messages)

        # Remove thread stats
        self.session().query(ThreadStatus).delete()
        self.session().commit()

    def console(self, text):
        print(text)

    def info(self, message, text=None, level='INFO'):
        # Write to console
        if self.verbose or level == 'ERROR':
            if text:
                self.console('[%s] %s: %s' % (current_thread().name, message, text))
            else:
                self.console('[%s] %s' % (current_thread().name, message))

        # Write to DB
        with self.lock:
            entry = LogEntry(type=level,
                             message_label=message,
                             text=text,
                             thread=current_thread().name)
            self.session().add(entry)
            self.session().commit()

    def error(self, text):
        self.info('ERROR', text, 'ERROR')

    def debug(self, text):
        self.info('DEBUG', text, 'DEBUG')

    def status(self, status, parsed, added, downloaded, start_time):
        with self.lock:
            stat = self.session().query(ThreadStatus).filter_by(thread=current_thread().name).one_or_none()
            if stat is None:
                # TODO log actual running time
                stat = ThreadStatus(thread=current_thread().name,
                                    status=status,
                                    parsed=parsed,
                                    added=added,
                                    downloaded=downloaded,
                                    running_time=round(time.time() - start_time, 0))
                self.session().add(stat)
            else:
                stat.status = status
                stat.parsed = parsed
                stat.added = added
                stat.downloaded = downloaded
                stat.running_time = round(time.time() - start_time, 0)

            self.session().commit()

    def some_running(self):
        with self.lock:
            return self.session().query(ThreadStatus).filter_by(status='RUNNING').count() > 0
