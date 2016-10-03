from importlib import import_module
from optparse import OptionParser
from db.model import Resource
from engine.queue import Queue
from engine.dispatcher import Dispatcher
import time
from engine.logger import Logger

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
    print('Montycrawler. Copyright 2016 Jose A. Brihuega Parodi <jose.brihuega@uca.es>')
    print('This is free software. See LICENSE.md for details.')
    # Parse options
    opt_parser = OptionParser('usage: python %prog [options] [URL]')
    opt_parser.add_option('-r', '--reset', dest='reset',
                          action='store_true',
                          help='reset database (data will be LOST!)')
    opt_parser.add_option('-p', '--preserve-queue', dest='preserve_queue',
                          action='store_true',
                          help="don't remove pending queue (if URL is provided)")
    opt_parser.add_option('--parser', dest='parser',
                          help="use CLASS to parse content (default SimpleParser)", metavar="CLASS",
                          default='parsing.SimpleParser')
    opt_parser.add_option('-a', '--all-domains', dest='all_domains',
                          action='store_true',
                          help='add resources from any domain (default only from the same base domain)')
    opt_parser.add_option('-t', '--threads', type='int', dest='threads', default=10,
                          help='number of threads (default 10)')
    opt_parser.add_option('-R', '--retries', type='int', dest='retries', default=3,
                          help='number of retries until resource in queue is discarded (default 3)')
    opt_parser.add_option('-d', '--depth', type='int', dest='depth', default=5,
                          help='max depth in link search (default 5)')
    opt_parser.add_option('-v', '--verbose', dest='verbose',
                          action='store_true',
                          help='verbose output')
    (options, args) = opt_parser.parse_args()

    # Start logger
    logger = Logger(options.verbose)
    logger.console('Process started at %s' % time.strftime("%b %d %Y - %H:%M:%S", time.localtime(start_time)))
    # Obtain queue
    queue = Queue(options.reset, options.all_domains, options.retries)
    if options.reset:
        logger.console('Database wiped.')

    # Section A: Add URL to pending queue
    if len(args) == 1:
        # Remove queue
        if not options.preserve_queue:
            n = queue.clear()
            logger.console('Empty queue. %d items deleted.' % n)
        # Insert URL
        res = Resource(url=args[0])
        (item, exists) = queue.add(res)
        if not exists:
            logger.console('URL "%s" already on queue.' % item.resource.url)
        else:
            logger.console('URL "%s" added to the queue.' % item.resource.url)

    # Section B: Get parser
    # Import given class name
    module_path, _, class_name = options.parser.rpartition('.')
    mod = import_module(module_path)
    parser = getattr(mod, class_name)
    logger.console('Parser %s loaded.' % parser.__name__)

    # Section B: Process queue
    # We will start dispatcher's threads with a random interval
    logger.console('%d resources in the pending queue.' % len(queue))
    # Start all threads
    threads = []
    for i in range(0, options.threads):
        # Each thread gets its own parser instance
        d = Dispatcher(queue, parser(), logger, max_depth=options.depth)
        d.start()
        threads.append(d)
    logger.console('Started %d threads.' % len(threads))
    # Wait all for termination
    for t in threads:
        t.join()

    logger.console('Exiting.  Process completed at %s in %d seconds.' %
                   (time.strftime('%b %d %Y - %H:%M:%S', time.localtime()), round(time.time() - start_time, 2)))



