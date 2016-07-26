from importlib import import_module
from optparse import OptionParser
from db.model import Resource
from engine.queue import Queue
from engine.dispatcher import Dispatcher
import time
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
	
# Main program
if __name__ == '__main__':
    start_time = time.time()
    # Parse options
    opt_parser = OptionParser('usage: python %prog [options] [URL]')
    opt_parser.add_option('-r', '--reset', dest='reset',
                          action='store_true',
                          help='reset database (data will be LOST!)')
    opt_parser.add_option('-p', '--preserve-queue', dest='preserve_queue',
                          action='store_true',
                          help="don't remove pending queue (if URL is provided)")
    opt_parser.add_option('--parser', dest='parser',
                      help="use CLASS to parse content", metavar="CLASS",
                          default='parsing.SimpleParser')
    opt_parser.add_option('-t', '--threads', type='int', dest='threads', default=10,
                          help='number of threads')
    (options, args) = opt_parser.parse_args()

    # Obtain queue
    queue = Queue(options.reset)
    if options.reset:
        print('Database wiped.')

    # Section A: Add URL to pending queue
    if len(args) == 1:
        # Remove queue
        if not options.preserve_queue:
            n = queue.clear()
            print('Empty queue. %d items deleted.' % n)
        # Insert URL
        res = Resource(url=args[0])
        (item, exists) = queue.add(res)
        if exists:
            print('URL "%s" already on queue.' % item.resource.url)
        else:
            print('URL "%s" added to the queue.' % item.resource.url)

    # Section B: Get parser
    # Import given class name
    module_path, _, class_name = options.parser.rpartition('.')
    mod = import_module(module_path)
    parser = getattr(mod, class_name)
    print('Parser %s loaded.' % parser.__name__)

    # Section B: Process queue
    # We will start dispatcher's threads with a random interval
    print('%d resources in the pending queue.'% len(queue))
    if len(queue) > 0:
        # Start all threads
        threads = []
        for i in range(0, options.threads):
            # Each thread gets its own parser instance
            d = Dispatcher(queue, parser())
            d.start()
            threads.append(d)
            # Random time between 1 and 5 seconds
            time.sleep(randint(1, 5))

        # Wait all for termination
        for t in threads:
            t.join()

    print('Exiting.  Process completed in %d seconds.' % round(time.time() - start_time, 2))




