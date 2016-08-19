#!/usr/bin/env python
# encoding: utf-8
import json
import sys
from os.path import join, dirname
from datetime import datetime

from wikicount.lib import Source, Bundle, Processor

DEFAULT_CONFIG_DIRECTORY = join(dirname(__file__), "../conf")
DEFAULT_CONFIG_FILE = join(DEFAULT_CONFIG_DIRECTORY, "config.json")


def compute(args, config=DEFAULT_CONFIG_FILE):
    print "Loading config file..."
    config = json.load(open(config))
    source = Source(*args, **config)
    processor = Processor(source)
    results = processor.process()
    print "Sending results..."
    processor.send_results(results, source.start_datetime, source.end_datetime)

if __name__ == '__main__':
    args = sys.argv
    if len(args) == 3:
        real_args = [datetime.strptime(args[i], "%d/%m/%y %H") for i in range(1,3)]
        compute(*real_args)
    elif len(args) == 2:
        arg = [datetime.strptime(args[1], "%d/%m/%y %H") for _ in range(2)]
        compute(arg)
    else:
        now = datetime.now()
        other = now.replace(month=now.month - 1)
        compute([other for _ in range(2)])
