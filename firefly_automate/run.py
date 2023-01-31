#!/bin/env python
import os
import logging
import pickle
from typing import Dict, List, Set
import argparse, argcomplete

import tqdm
from datetime import datetime
from dateutil.parser import parse as dateutil_parser
from dateutil.relativedelta import relativedelta
from icecream import ic

from firefly_automate import rules
import firefly_automate.rules.base_rule
from firefly_automate.firefly_request_manager import (
    get_transactions,
    send_transaction_delete,
)
from firefly_automate.miscs import (
    FireflyTransactionDataClass,
    PendingUpdates,
    group_by,
    prompt_response,
)

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
LOGGER = logging.getLogger()

pending_updates: Dict[int, PendingUpdates] = {}
pending_deletes: Set[int] = set()
all_rules = [
    cls(
        pending_updates=pending_updates, pending_deletes=pending_deletes  # type: ignore
    )
    for cls in rules.base_rule.Rule.__subclasses__()
    if cls.enable_by_default
]
all_rules_name = list(map(lambda r: r.base_name, all_rules))
available_rules = list(filter(lambda x: x.enable_by_default, all_rules))
available_rules_name = list(map(lambda r: r.base_name, available_rules))

parser = argparse.ArgumentParser()
parser.add_argument(
    "--yes",
    action="store_true",
    help="Assume yes to all confirmations",
)
parser.add_argument(
    "--run",
    choices=all_rules_name,
    help="Only run the specified rule",
    type=str,
)
parser.add_argument(
    "-d",
    "--disable",
    default=[],
    nargs="+",
    choices=available_rules_name,
    help="Disable the following rules",
    type=str,
)
parser.add_argument(
    "--list-rules",
    action="store_true",
    help="List all available rules' base-name and then exit",
)
parser.add_argument(
    "-s",
    "--start",
    default=(datetime.now() - relativedelta(month=3)).date(),
    help="Start date for the range of transactions to process (default 3 months ago)",
    type=lambda x: dateutil_parser(x).date(),
)
parser.add_argument(
    "-e",
    "--end",
    default=datetime.now().date(),
    help="End date for the range of transactions to process",
    type=lambda x: dateutil_parser(x).date(),
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
    "--rule-config",
    default="",
    help="String that pass to rule backend",
    type=str,
)
parser.add_argument(
    "--debug",
    default=False,
    help="Debug logging",
    action="store_true",
)
argcomplete.autocomplete(parser)


def setup_logger():
    # create logger
    logger = logging.getLogger("project")
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel("DEBUG")

    # create formatter
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)


def main():
    global available_rules
    args = parser.parse_args()

    if args.list_rules:
        print("\n".join(all_rules_name))
        return
    if args.run:
        available_rules = list(filter(lambda x: x.base_name == args.run, all_rules))
    if args.debug:
        LOGGER.setLevel(logging.DEBUG)
        setup_logger()
    else:
        ic.disable()

    def process_one_transaction(entry: FireflyTransactionDataClass):

        try:
            for rule in available_rules:
                if rule.base_name in args.disable:
                    continue
                rule.process(entry)
        except rules.base_rule.StopRuleProcessing:
            pass

    if not args.use_cache or not os.path.exists(args.cache_file_name):
        all_transactions = list(get_transactions(args.start, args.end))

        # if args.use_cache:
        with open(args.cache_file_name, "wb") as f:
            pickle.dump(all_transactions, f)
    else:
        with open(args.cache_file_name, "rb") as f:
            all_transactions = pickle.load(f)

    ic(all_transactions)

    for rule in available_rules:
        rule.set_all_transactions(all_transactions)
        rule.set_rule_config(args.rule_config)
    # TODO: make this parallel if num of transactions is huge
    for data in all_transactions:
        process_one_transaction(data)

    print("========================")

    if len(pending_updates) == 0 and len(pending_deletes) == 0:
        print("No update necessary.")
        exit()

    elif len(pending_updates) > 0:
        for acc, updates_in_one_acc in group_by(
            pending_updates.values(), lambda x: x.acc
        ).items():
            print(f"{acc}:")
            grouped_rule_updates = group_by(updates_in_one_acc, lambda x: x.rule)
            for rule_name, updates_in_one_rule in grouped_rule_updates.items():

                print(f" >> rule: {rule_name} <<")
                for updates in sorted(updates_in_one_rule, key=lambda x: x.date):
                    print(updates)

        print("=========================")
        if not args.yes:
            prompt_response(
                ">> IMPORTANT: Review the above output and see if the updates are ok:"
            )

        for updates in tqdm.tqdm(pending_updates.values(), desc="Applying updates"):
            updates.apply(dry_run=False)

    elif len(pending_deletes) > 0:
        if not args.yes:
            prompt_response(">> Ready to perform the delete?")
        for deletes_id in tqdm.tqdm(pending_deletes, desc="Applying deletes"):
            send_transaction_delete(int(deletes_id))


if __name__ == "__main__":
    main()
