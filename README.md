postfix-stats-collector
=======================
Postfix stats collector (StatsD integration)

Stats are build based on two data sources.
- Firstly it listens on stdin/parses files for postfix logs.
- Secondly it checks once a 10 seconds for the length of postfix queues processing output of qshape.

Postfix logging parser is based on: https://github.com/disqus/postfix-stats


configuration
-------------
Following variables can be used to manage statsd destination and namespace
- STATSD_HOST=localhost
- STATSD_PORT=8125
- STATSD_PREFIX=None
- STATSD_MAXUDPSIZE=512
To configure frequency of qshape measurements:
- STATSD_DELAY=10


TODO
----
- syslog configuraiton examples
- when log parser finishes, it should ask qshape to finish as well
- add mailer test script (parse DNS, pull MX records and test connection (HELO/EHLO))
