postfix-stats-collector
=======================
Postfix stats collector (StatsD integration)

Stats are build based on two data sources.
- Firstly it listens on stdin/parses files for postfix logs
  `postfix-stats-logparser`
- Secondly it checks once a 10 seconds for the length of postfix queues processing output of qshape
  `postfix-stats-qshape`

Postfix logging parser is based on: https://github.com/disqus/postfix-stats


installation
------------
`pip install git+https://github.com/ministryofjustice/postfix-stats-collector.git#egg=postfix-stats-collector`


executables
-----------
- `postfix-stats-logparser`
- `postfix-stats-qshapes`

configuration
-------------
Following variables can be used to manage statsd destination and namespace
- STATSD_HOST=localhost
- STATSD_PORT=8125
- STATSD_PREFIX=None
- STATSD_MAXUDPSIZE=512
To configure frequency of qshape measurements:
- STATSD_DELAY=10


example syslog-ng configuration
-------------------------------
```
destination df_postfix_stats { program("/usr/local/bin/postfix-stats-logparser"); };

filter f_postfix { program("^postfix/"); };

log {
    source(s_src);
    filter(f_mail);
    filter(f_postfix);
    destination(df_postfix_stats);
};
```


TODO
----
- more syslog configuraiton examples
- add mailer test script (parse DNS, pull MX records and test connection - HELO/EHLO)
