"""
Monitoris postfix and reports metrics to statsd.
Supported enviroment variables and their respective defaults:
STATSD_HOST=localhost
STATSD_PORT=8125
STATSD_PREFIX=None
STATSD_MAXUDPSIZE=512
"""

from gevent import monkey
monkey.patch_all()

import signal
import sys
import argparse
import threading
import logging
from functools import partial
import multiprocessing

from qshape import process_qshape
from logparser import process_log_files


logger = logging.getLogger(__name__)


def argparse_maker():
    """
    :return: argparse object
    """
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-v", "--verbose", dest="verbosity", default=0, action="count",
                        help="-v for a little info, -vv for debugging")
    parser.add_argument("-c", "--concurrency", dest="concurrency", default=multiprocessing.cpu_count(), type=int,
                        metavar="threads",
                        help="Number of threads to spawn for handling lines")
    parser.add_argument("--skip-qshape", dest="skip_qshape", default=False, action='store_true',
                        help="Skip qshape monitoring")
    parser.add_argument("--skip-log-parser", dest="skip_logparser", default=False, action='store_true',
                        help="Skip log parsing")
    parser.add_argument("-l", "--local", dest="local_emails", default=[], action="append",
                        metavar="local_emails",
                        help="Search for STRING in incoming email addresses and incr stat NAME and if COUNT, count in incoming - STRING,NAME,COUNT")
    parser.add_argument('log_files', metavar='file', type=str, nargs='*', default="-",
                        help='an integer for the accumulator')
    return parser


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)
    #TODO: signall all threads that it's time to die


def processor(log_files, concurrency=2, local_emails=None, skip_qshape=False, skip_logparser=False):
    """
    initiate all stats processors
    :param log_files:
    :param concurrency:
    :param local_emails:
    :return:
    """
    partial_process_log_files = partial(process_log_files, log_files, concurrency, local_emails)
    tasks = []
    if not skip_qshape:
        tasks.append(process_qshape)
    if not skip_logparser:
        tasks.append(partial_process_log_files)

    threads = []
    for task in tasks:
        t = threading.Thread(target=task)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


def main():
    parser = argparse_maker()
    args = parser.parse_args()
    assert args.verbosity is not None
    assert args.concurrency is not None
    assert args.local_emails is not None
    assert args.skip_qshape is not None
    assert args.skip_logparser is not None
    assert args.log_files is not None

    if args.verbosity == 0:
        logging.basicConfig(level=logging.WARNING)
    elif args.verbosity == 1:
        logging.basicConfig(level=logging.INFO)
    elif args.verbosity >= 2:
        logging.basicConfig(level=logging.DEBUG)

    # signal.signal(signal.SIGINT, signal_handler)
    # print('Press Ctrl+C to exit')

    processor(log_files=args.log_files, concurrency=args.concurrency, local_emails=args.local_emails,
              skip_qshape=args.skip_qshape, skip_logparser=args.skip_logparser)


if __name__ == '__main__':
    main()
