#!/bin/env python
import os
import logging
import pickle
from typing import Dict, List, Set
import argparse

import tqdm

from firefly_automate import rules
from firefly_automate.config_loader import config
from firefly_automate.firefly_request_manager import (
    send_transaction_delete,
)
from firefly_automate.miscs import (
    FireflyTransactionDataClass,
    PendingUpdates,
    group_by,
    prompt_response,
)

LOGGER = logging.getLogger()

command_name = "transform"

pending_updates: Dict[int, PendingUpdates] = {}
pending_deletes: Set[int] = set()

all_rules = [
    cls(
        pending_updates=pending_updates, pending_deletes=pending_deletes  # type: ignore
    )
    for cls in rules.base_rule.Rule.__subclasses__()
]
all_rules_name = list(map(lambda r: r.base_name, all_rules))
available_rules = list(filter(lambda x: x.enable_by_default, all_rules))
available_rules_name = list(map(lambda r: r.base_name, available_rules))


def init_subparser(parser):
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
        "--rule-config",
        default="",
        help="String that pass to rule backend",
        type=str,
    )
    parser.add_argument(
        "--list-rules",
        action="store_true",
        help="List all available rules' base-name and then exit",
    )


def run(args: argparse.ArgumentParser):
    global available_rules

    if args.list_rules:
        print("\n".join(all_rules_name))
        return
    if args.run:
        available_rules = list(filter(lambda x: x.base_name == args.run, all_rules))

    def process_one_transaction(entry: FireflyTransactionDataClass):
        try:
            for rule in available_rules:
                if rule.base_name in args.disable:
                    continue
                rule.process(entry)
        except rules.base_rule.StopRuleProcessing:
            pass

    all_transactions = args.get_transactions()

    for rule in available_rules:
        rule.set_all_transactions(all_transactions)
        rule.set_rule_config(args.rule_config)
    # TODO: make this parallel if num of transactions is huge
    for data in filter(
        lambda t: t.id not in config["ignore_transaction_ids"], all_transactions
    ):
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
        if args.yes or prompt_response(
            ">> IMPORTANT: Review the above output and see if the updates are ok:"
        ):

            for updates in tqdm.tqdm(pending_updates.values(), desc="Applying updates"):
                updates.apply(dry_run=False)

    elif len(pending_deletes) > 0:
        if args.yes or prompt_response(">> Ready to perform the delete?"):
            for deletes_id in tqdm.tqdm(pending_deletes, desc="Applying deletes"):
                send_transaction_delete(int(deletes_id))
