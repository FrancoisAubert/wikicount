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
    args = sys.argv.pop(0)
    if len(args) == 2:
        args = args.map(lambda arg: datetime.strptime(arg, "%d/%m/%y %H"))
        compute(*args)
    elif len(args) == 1:
        args = args.map(lambda arg: datetime.strptime(arg, "%d/%m/%y %H"))
        compute([args[0] for _ in range(2)])
    else:
        now = datetime.now()
        other = now.replace(month=now.month - 1)
        compute([other for _ in range(2)])
