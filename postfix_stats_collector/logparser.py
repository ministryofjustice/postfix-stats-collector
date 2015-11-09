import re
import sys
import time
import logging
import fileinput
from collections import defaultdict, Iterator
from Queue import Queue, Full
from threading import Thread, Lock

from statsd.defaults.env import statsd

logger = logging.getLogger(__name__)

retype = type(re.compile('nothing'))

local_addresses = dict()
local_addresses_re = None


class StdinReader(Iterator):
    def next(self):
        try:
            line = sys.stdin.readline()
        except KeyboardInterrupt:
            raise StopIteration

        if not line:
            raise StopIteration

        return line

    def isstdin(self):
        return True


class Handler(object):
    """
    Class representing abstract log handler. Never instantiate directly, always inherit to implement a specific log
    handler.
    To enable handler, just instantiate class and object will self-register within global Handler.handlers
    """
    filter_re = re.compile(r'(?!)')
    facilities = None
    handlers = defaultdict(list)  # a class variable; global dictionary of facility -> [handler,...]

    def __init__(self, *args, **kwargs):
        assert self.__class__.__name__ != 'Handler'
        assert isinstance(self.filter_re, retype)
        self.register(self.facilities)

    def parse(self, line):
        pline = self.filter_re.match(line)

        if pline:
            logger.debug(pline.groupdict())
            self.handle(**pline.groupdict())

    def handle(self, **kwargs):
        raise NotImplementedError()

    def register(self, facilities):
        facilities = set(facilities)
        for facility in facilities:
            if self not in Handler.handlers[facility]:
                Handler.handlers[facility].append(self)

        self.facilities |= facilities


class BounceHandler(Handler):
    facilities = set(['bounce'])
    filter_re = re.compile((r'\A(?P<message_id>\w+?): sender non-delivery notification: (?P<bounce_message_id>\w+?)\Z'))

    def handle(self, message_id=None, bounce_message_id=None):
        statsd.incr('postfix.messages.bounce', 1)


class CleanupHandler(Handler):
    facilities = set(['cleanup'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): message-id=\<(?P<ext_message_id>.+?)\>\Z')

    def handle(self, message_id=None, ext_message_id=None):
        statsd.incr('postfix.messages.cleanup', 1)


class LocalHandler(Handler):
    facilities = set(['local'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): to=\<(?P<to_email>.*?)\>, orig_to=\<(?P<orig_to_email>.*?)\>, relay=(?P<relay>.+?), delay=(?P<delay>[0-9\.]+), delays=(?P<delays>[0-9\.\/]+), dsn=(?P<dsn>[0-9\.]+), status=(?P<status>\w+) \((?P<response>.+?)\)\Z')
    local_addresses_re = re.compile(r'(?!)')

    def __init__(self, *args, **kwargs):
        super(LocalHandler, self).__init__(*args, **kwargs)

        if local_addresses_re:
            assert isinstance(local_addresses_re, retype)
            self.local_addresses_re = local_addresses_re
        self.local_addresses = local_addresses

    def handle(self, message_id=None, to_email=None, orig_to_email=None, relay=None, delay=None, delays=None, dsn=None, status=None, response=None):
        pemail = self.local_addresses_re.search(to_email)

        if pemail:
            search = pemail.group(1)

            name, count = self.local_addresses[search]

            logger.debug('Local address <%s> count (%s) as "%s"', search, count, name)

            statsd.incr('postfix.messages.local', 1)
            statsd.incr('postfix.messages.in.status.{}'.format(status), 1)
            statsd.incr('postfix.messages.in.resp_codes.{}'.format(dsn.replace(".", "_")), 1)   # we don't want to create tree from domain name so "." are forbidden
            # with stats_lock:
            #     stats['local'][name] += 1
            #     if count:
            #         stats['in']['status'][status] += 1
            #         stats['in']['resp_codes'][dsn] += 1


class QmgrHandler(Handler):
    facilities = set(['qmgr'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): (?:(?P<removed>removed)|(?:from=\<(?P<from_address>.*?)\>, size=(?P<size>[0-9]+), nrcpt=(?P<nrcpt>[0-9]+) \(queue (?P<queue>[a-z]+)\)))?\Z')

    def handle(self, message_id=None, removed=None, from_address=None, size=None, nrcpt=None, queue=None):
        pass


class SmtpHandler(Handler):
    facilities = set(['smtp', 'error'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): to=\<(?P<to_email>.+?)\>, relay=(?P<relay>.+?), (?:conn_use=(?P<conn_use>\d), )?delay=(?P<delay>[0-9\.]+), delays=(?P<delays>[0-9\.\/]+), dsn=(?P<dsn>[0-9\.]+), status=(?P<status>\w+) \((?P<response>.+?)\)\Z')

    def handle(self, message_id=None, to_email=None, relay=None, conn_use=None, delay=None, delays=None, dsn=None, status=None, response=None):
        stat = 'recv' if '127.0.0.1' in relay else 'send'
        statsd.incr('postfix.messages.{}.status.{}'.format(stat, status), 1)
        statsd.incr('postfix.messages.{}.resp_codes.{}'.format(stat, dsn.replace(".", "_")), 1)   # we don't want to create tree from domain name so "." are forbidden

        # with stats_lock:
        #     stats[stat]['status'][status] += 1
        #     stats[stat]['resp_codes'][dsn] += 1


class SmtpdHandler(Handler):
    facilities = set(['smtpd'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): client=(?P<client_hostname>[.\w-]+)\[(?P<client_ip>[A-Fa-f0-9.:]{3,39})\](?:, sasl_method=[\w-]+)?(?:, sasl_username=[-_.@\w]+)?(?:, sasl_sender=\S)?(?:, orig_queue_id=\w+)?(?:, orig_client=(?P<orig_client_hostname>[.\w-]+)\[(?P<orig_client_ip>[A-Fa-f0-9.:]{3,39})\])?\Z')

    def handle(self, message_id=None, client_hostname=None, client_ip=None, orig_client_hostname=None, orig_client_ip=None):
        statsd.incr('postfix.messages.smtpd', 1)
        pass
        # ip = orig_client_ip or client_ip
        #
        # with stats_lock:
        #     stats['clients'][ip] += 1


def register_handlers(handlers=[BounceHandler, CleanupHandler, LocalHandler, QmgrHandler, SmtpHandler, SmtpdHandler]):
    """
    Function registering all available handlers
    :return: None
    """
    for handler in handlers:
        handler()


class Parser(Thread):
    """
    Worker thread parsing syslog formatted logs.
    It detests facility of log and then sends it to appropriate registered handler.
    """
    lines = None  # Queue object
    line_re = re.compile(r'\A(?P<iso_date>\D{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(?P<source>.+?)\s+(?P<facility>.+?)\[(?P<pid>\d+?)\]:\s(?P<message>.*)\Z')

    def __init__(self, lines):
        super(Parser, self).__init__()
        self.lines = lines
        self.daemon = True
        self.start()

    def run(self):
        while True:
            line = self.lines.get()

            try:
                self.parse_line(line)
            except Exception, e:
                logger.exception('Error parsing line: %s', line)
            finally:
                self.lines.task_done()

    def parse_line(self, line):
        pln = self.line_re.match(line)

        if pln:
            pline = pln.groupdict()
            logger.debug(pline)

            facility = pline['facility'].split('/')

            for handler in Handler.handlers[facility[-1]]:
                handler.parse(pline['message'])


class ParserPool(object):
    def __init__(self, num_parsers):
        """
        :param num_parsers: parsing threads
        :return:
        """
        self.lines = Queue(num_parsers * 1000)

        for i in xrange(num_parsers):
            logger.info('Starting parser %s', i)
            Parser(self.lines)

    def add_line(self, line, block=False):
        self.lines.put(line, block)

    def join(self):
        self.lines.join()


def process_log_files(log_files, concurrency=None, local_emails=None, running_event=None):
    print("Starting log parsing for: {}".format(reduce(lambda x, y: "{}, {}".format(x,y), log_files)))

    # handle local_emails
    global local_addresses_re
    global local_addresses
    true_values = ['yes', '1', 'true']
    for local_email in local_emails:
        try:
            search, name, count = local_email.strip('()').split(',')
        except ValueError:
            logger.error('LOCAL_TUPLE requires 3 fields: %s', local_email)
            return -1

        local_addresses[search] = (name, count.lower() in true_values)

    if local_addresses:
        local_addresses_re = re.compile(r'(%s)' % '|'.join(local_addresses.keys()))
        logger.debug('Local email pattern: %s', local_addresses_re.pattern)
    else:
        local_addresses_re = re.compile(r'(?!)')

    # initiate handlers
    register_handlers()

    # kick parser
    parser_pool = ParserPool(concurrency)
    if not log_files or log_files[0] is '-':
        reader = StdinReader()
    else:
        reader = fileinput.input(log_files)

    for line in reader:  # note that fileinput will not react to running_event.clear()
        if running_event is not None and not running_event.is_set():
            break
        try:
            parser_pool.add_line(line.strip('\n'), block=not reader.isstdin())
        except Full:
            logger.warning('Line parser queue full')
            time.sleep(0.1)

    parser_pool.join()
    print("Finished log parsing")

