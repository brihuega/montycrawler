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

"""Montycrawler: A multithreaded web crawler designed to obtain and analyze PDF documents.

Author:
    Jose A. Brihuega Parodi <jose.brihuega@uca.es>

Usage:
    python mc.py [options] [URL]
    Use option --help for details.

"""

from importlib import import_module
from optparse import OptionParser
from db.model import Resource
from engine.queue import Queue
from engine.dispatcher import Dispatcher
import time
import os
import errno
from engine.logger import Logger


def load_class(name):
    """Utility function to load a class.

    Args:
        name: Full class/module name.

    Returns:
        The loaded class.

    """
    module_path, _, class_name = name.rpartition('.')
    mod = import_module(module_path)
    return getattr(mod, class_name)


def create_folder(path):
    """Utility function to create folder if it doesn't exist.

    Args:
        path: The path.

    """
    try:
        os.makedirs(path)
        logger.console('Folder "%s" created.' % os.path.abspath(path))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

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
    opt_parser.add_option('--processor', dest='processor',
                          help="use CLASS to process documents (default PDFProcessor)", metavar="CLASS",
                          default='processing.PDFProcessor')
    opt_parser.add_option('-a', '--all-domains', dest='all_domains',
                          action='store_true',
                          help='add resources from any domain (default only from the same base domain)')
    opt_parser.add_option('-t', '--threads', type='int', dest='threads', default=10,
                          help='number of threads (default 10)')
    opt_parser.add_option('-R', '--retries', type='int', dest='retries', default=3,
                          help='number of threads (default 10)')
    opt_parser.add_option('-k', '--keywords', type='string', dest='keywords',
                          help='list of relevant keywords (comma separated without spaces)')
    opt_parser.add_option('-f', '--download-folder', type='string', dest='download_folder', default='files',
                          help='destination folder for downloaded files (default "files")')
    opt_parser.add_option('-F', '--rejected-folder', type='string', dest='rejected_folder',
                          help="destination folder for rejected files (default, don't store them)")
    opt_parser.add_option('-d', '--depth', type='int', dest='depth', default=5,
                          help='max depth in link search (default 5)')
    opt_parser.add_option('-m', '--min-relevancy', type='float', dest='min_relevancy', default=1,
                          help='Minimum relevancy score to accept documents (only if keywords supplied) (default 1)')
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

    # Check if download folders exist, otherwise create them
    create_folder(options.download_folder)
    if options.rejected_folder:
        create_folder(options.rejected_folder)

    # Split list of keywords
    if options.keywords:
        keywords = [x.strip() for x in options.keywords.split(',')]
    else:
        keywords = None

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

    # Section B: Get parser and processor
    parser = load_class(options.parser)
    logger.console('Parser %s loaded.' % parser.__name__)
    processor = load_class(options.processor)
    logger.console('Processor %s loaded.' % processor.__name__)

    # Section C: Process queue
    # We will start dispatcher's threads with a random interval
    logger.console('%d resources in the pending queue.' % len(queue))
    # Start all threads
    threads = []
    for i in range(0, options.threads):
        # Each thread gets its own parser instance
        d = Dispatcher(queue, parser(keywords=keywords), processor(keywords=keywords), logger,
                       max_depth=options.depth,
                       download_folder=options.download_folder,
                       rejected_folder=options.rejected_folder,
                       min_relevancy=options.min_relevancy if keywords else 0)
        d.start()
        threads.append(d)
    logger.console('Started %d threads.' % len(threads))
    # Wait all for termination
    for t in threads:
        t.join()

    logger.console('Exiting.  Process completed at %s in %d seconds.' %
                   (time.strftime('%b %d %Y - %H:%M:%S', time.localtime()), round(time.time() - start_time, 2)))
