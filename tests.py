import time
import unittest
import fileinput
import mock
import Queue

from collections import defaultdict

from postfix_stats_collector.qshape import get_qshape_stats
from postfix_stats_collector.logparser import ParserPool, register_handlers, Handler
from pprint import pprint

STATIC_QSHAPE = """
                               T  5 10 20 40 80 160 320 640 1280 2560 5120 5120+
                        TOTAL  6  0  3  0  2  0   0   0   1    0    0    0     0
                  example.com  7  0  3  0  2  0   0   0   2    0    0    0     0
"""

STATIC_LOG = """
"""

mock_check_output = mock.Mock(side_effect=lambda x: STATIC_QSHAPE)


class TestQshapeParser(unittest.TestCase):
    @mock.patch("subprocess.check_output", mock_check_output)
    def test_parser(self):
        self.assertIn('TOTAL', mock_check_output([]))

        stats = list(get_qshape_stats(limit_top_domains=10))
        dict_stats = dict(stats)
        self.assertIn('postfix.qshape.active.total.sum', dict_stats)
        self.assertEqual(dict_stats['postfix.qshape.active.total.sum'], 6)
        self.assertEqual(dict_stats['postfix.qshape.active.total.5'], 6)
        self.assertEqual(dict_stats['postfix.qshape.active.total.10'], 3)
        self.assertEqual(dict_stats['postfix.qshape.active.total.20'], 3)
        self.assertEqual(dict_stats['postfix.qshape.active.total.40'], 1)
        self.assertNotIn('postfix.qshape.active.total.5120+', dict_stats)
        self.assertEqual(dict_stats['postfix.qshape.active.example_com.10'], 4)
        self.assertIn('postfix.qshape.processing_time', dict_stats)


class TestLogParser(unittest.TestCase):
    def test_handler_registration(self):
        register_handlers()
        self.assertTrue(Handler.handlers)

    @mock.patch('postfix_stats_collector.logparser.statsd')
    def test_lines(self, statsd_mock):
        statsd_data = defaultdict(int)

        def statsd_mock_incr(k, v):
            statsd_data[k] += v
        statsd_mock.incr.side_effect = statsd_mock_incr

        logreader = fileinput.input("mail.log")
        parser_pool = ParserPool(10)
        for line in logreader:
            try:
                parser_pool.add_line(line.strip('\n'), block=True)
            except Queue.Full:
                time.sleep(0.1)

        parser_pool.join()
        self.assertTrue(statsd_mock.incr.called)
        self.assertIn(mock.call('postfix.messages.bounce', 1), statsd_mock.incr.mock_calls)  #
        self.assertDictContainsSubset({
            'postfix.messages.bounce': 1,
            'postfix.messages.cleanup': 3,
            'postfix.messages.send.resp_codes.5_4_4': 2,
            'postfix.messages.send.status.bounced': 2
            },
            statsd_data)


if __name__ == '__main__':
    unittest.main()
