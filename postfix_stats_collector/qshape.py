import os
import time
import schedule
import subprocess
import logging
from itertools import ifilter

from statsd.defaults.env import statsd

logger = logging.getLogger(__name__)

STATSD_DELAY = int(os.environ.get("STATSD_DELAY", 10))

QUEUES = ["maildrop",
          "hold",
          "incoming",
          "active",
          "deferred"]


def get_qshape_stats(limit_top_domains=0):
    """
    Executes postfix `qshape` on every queue and transforms its output for monitoring usage.

    `qshape` is a brilliant to have an insight into current state of system, but with whole monitoring backend
    we are getting ability to peek into historical data and chart it.

    As `qshape` answers the question of bucket size:
      "How many emails were waiting in the queue from 20 to 40 mins?"
    We are transforming the output so that it optimises to answer question:
      "How many emails were waiting in the queue for more then 20 minutes?".

    Information if 10 emails are waiting less then 40 mins gives a good insight into a current state of system,
    but that's not a type of question you'll set an alert on.
    Also this way when we chart metrics, we see a gradual growth in opposition to rapid data moving between graphs.

    Note that this transformation is not removing any information and as such can be reversed anytime later.
    Note also that storing what is the number of emails in the queue above "5120+" doesn't make any sense.
    "5120+" means infinity in our case and there will always be 0 email waiting in the queue more than infinity of time.


    Example output from qshape:
                                   T  5 10 20 40 80 160 320 640 1280 2560 5120 5120+
                            TOTAL  4  0  0  0  2  0   2   0   0    0    0    0     0
                      example.com  6  0  0  0  2  0   2   0   0    0    0    0     2

    Transformation applied:
                                   T  5 10 20 40 80 160 320 640 1280 2560 5120
                            TOTAL  4  4  4  4  2  2   0   0   0    0    0    0
                      example.com  6  6  6  6  4  4   2   2   2    2    2    2

    :param limit_top_domains: limit how many top domains should be reported as separate metrics (default=0)
    :return: list of stats [(key, value),...], where key is: "postfix.qshape.{queue}.{domain}.{bucket}"
    """
    t0 = time.time()
    for queue in QUEUES:
        # iterate on non empty lines from qshape output
        logger.debug("working on qshape queue: {}".format(queue))
        lines = ifilter(lambda x: x,
                        subprocess.check_output(['/usr/sbin/qshape', '-n', str(limit_top_domains), '-b', '12', queue]).splitlines())
        logger.debug("qshape output: {}".format(lines))
        header_line = lines.next().strip()
        headers = header_line.split()
        assert headers[0] == 'T'
        headers = headers[1:]
        for line in lines:
            values = line.split()
            domain = values[0].lower()  # 1st entry is always TOTAL
            total_sum = int(values[1])
            yield ("postfix.qshape.{queue}.{domain}.{bucket}".format(queue=queue, domain=domain, bucket='sum'), int(total_sum))

            values = values[2:]  # get rid of domain and sum

            # get from:  0  1  2  1  0 10 0 0
            # to:       14 13 11 10 10  0 0
            values_inverted_summed = values  # let's initiate the size
            last_value = total_sum
            for i in range(len(values)):
                last_value -= int(values[i])
                values_inverted_summed[i] = last_value

            domain = domain.replace(".", "_")  # we don't want to create tree from domain name so "." are forbidden
            # we don't report 5120+ value as it does not add any value
            # it's always zero as we deliver everything before the nd of times
            for i in range(len(headers)-1):
                bucket = headers[i]
                value = values_inverted_summed[i]  # skip the title of the row
                yield ("postfix.qshape.{queue}.{domain}.{bucket}".format(queue=queue, domain=domain, bucket=bucket), value)
    t1 = time.time()
    yield "postfix.qshape.processing_time", t1-t0


def process_qshape():
    running = True
    print("Starting qshape processing")

    def report_stats():
        with statsd.pipeline() as pipe:
            for stat, value in get_qshape_stats():
                pipe.incr(stat, value)

    report_stats()  # report current metrics and schedule them to the future
    schedule.every(STATSD_DELAY).seconds.do(report_stats)
    while running:
        schedule.run_pending()
        time.sleep(0.1)
