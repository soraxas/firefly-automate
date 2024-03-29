#!/bin/env python
import argparse
import logging
import os
import pickle
from datetime import datetime

import argcomplete
from dateutil.parser import parse as dateutil_parser
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
    type=lambda x: dateutil_parser(x, dayfirst=True).date(),
)
parser.add_argument(
    "-e",
    "--end",
    default=None,
    help="End date for the range of transactions to process",
    type=lambda x: dateutil_parser(x, dayfirst=True).date(),
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


def _get_transactions(args: argparse.ArgumentParser):
    if not args.use_cache or not os.path.exists(args.cache_file_name):
        all_transactions = list(get_transactions(args.start, args.end))

        # if args.use_cache:
        with open(args.cache_file_name, "wb") as f:
            pickle.dump(all_transactions, f)
    else:
        with open(args.cache_file_name, "rb") as f:
            all_transactions = pickle.load(f)

    LOGGER.debug(all_transactions)
    return all_transactions


def init(args: argparse.ArgumentParser):
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
    miscs.always_override_reconciled = args.always_override_reconciled

    setup_logger(args.debug)


COMMANDS_MODULES = (
    run_transform_transactions,
    run_merge_transfer,
    run_import_csv,
)

args = None
parser.set_defaults(get_transactions=lambda: _get_transactions(args))

subparser = parser.add_subparsers(dest="command")
for _subcommand_module in COMMANDS_MODULES:
    _sub_parser = subparser.add_parser(_subcommand_module.command_name)
    _subcommand_module.init_subparser(_sub_parser)


########################################################
argcomplete.autocomplete(parser)
########################################################


def main():
    global args
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
