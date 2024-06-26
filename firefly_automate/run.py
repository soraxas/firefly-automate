#!/bin/env python
import argparse
import logging
import os
import shelve
from datetime import datetime

import argcomplete
from dateutil.relativedelta import relativedelta

from firefly_automate.config_loader import config
from firefly_automate.firefly_request_manager import get_transactions
from firefly_automate.miscs import setup_logger

from . import miscs
from .commands import run_import_csv, run_merge_transfer, run_transform_transactions

LOGGER = logging.getLogger()


parser = argparse.ArgumentParser()
parser.add_argument(
    "--yes",
    action="store_true",
    help="Assume yes to all confirmations",
)
parser.add_argument(
    "-s",
    "--start",
    default=None,
    help="Start date for the range of transactions to process (default 3 months ago)",
    type=miscs.my_dateutil_parser,
)
parser.add_argument(
    "-e",
    "--end",
    default=None,
    help="End date for the range of transactions to process",
    type=miscs.my_dateutil_parser,
)
parser.add_argument(
    "--wait-for-all-transaction",
    action="store_true",
    help="If set, wait for all transactions' arrival before applying rules",
)
parser.add_argument(
    "--cache-file-name",
    default="__firefly-iii_automate_cache.pkl",
    help="File name to be used for cache purpose.",
    type=str,
)
parser.add_argument(
    "--use-cache",
    action="store_true",
    help="If set, store and use previously stored cache file",
)
parser.add_argument(
    "-m",
    "--relative-months",
    default=3,
    type=int,
    help="If `start` or `end` is not given, a relative time of this many months will be used.",
)
parser.add_argument(
    "--debug",
    default=False,
    help="Debug logging",
    action="store_true",
)
parser.add_argument(
    "--always-override-reconciled",
    default=False,
    help="Debug logging",
    action="store_true",
)

########################################################


def _get_transactions():
    global ARGS

    transaction_key = str(("transaction", ARGS.start, ARGS.end))

    if transaction_key not in ARGS.cache:
        ARGS.cache[transaction_key] = list(get_transactions(ARGS.start, ARGS.end))

    LOGGER.debug(ARGS.cache[transaction_key])
    return ARGS.cache[transaction_key]


def init(args: argparse.Namespace):
    global ARGS
    ARGS = args
    ARGS.cache = shelve.open(ARGS.cache_file_name)
    if ARGS.use_cache:
        pass
    else:
        ARGS.cache.clear()
    ####################################
    # if all is None, default to most recent 3 months
    if all(x is None for x in (args.start, args.end)):
        args.end = datetime.now().date()
    if args.start is None and args.end is not None:
        args.start = args.end - relativedelta(months=args.relative_months)
    elif args.start is not None and args.end is None:
        args.end = args.start + relativedelta(months=args.relative_months)
    LOGGER.debug("From: {} to {}", args.start, args.end)
    ####################################
    miscs.set_args(args)

    setup_logger(args.debug)


COMMANDS_MODULES = (
    run_transform_transactions,
    run_merge_transfer,
    run_import_csv,
)

ARGS: argparse.Namespace = None
parser.set_defaults(get_transactions=_get_transactions)

subparser = parser.add_subparsers(dest="command")
for _subcommand_module in COMMANDS_MODULES:
    _sub_parser = subparser.add_parser(_subcommand_module.command_name)
    _subcommand_module.init_subparser(_sub_parser)


########################################################
argcomplete.autocomplete(parser)
########################################################


def main():
    args = parser.parse_args()
    init(args)

    for _module in COMMANDS_MODULES:
        if args.command == _module.command_name:
            _module.run(args)
            break
    else:
        parser.print_usage()
        exit(1)


if __name__ == "__main__":
    main()
