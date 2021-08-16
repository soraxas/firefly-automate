from typing import Dict
import argparse

import tqdm
from dateutil.parser import parse as dateutil_parser

import rules
import rules.base_rule
from firefly_request_manager import get_transactions
from miscs import (
    FireflyTransactionDataClass,
    PendingUpdates,
    group_by,
)

pending_updates: Dict[int, PendingUpdates] = {}
available_rules = [
    cls(pending_updates) for cls in rules.base_rule.Rule.__subclasses__()
]
available_rules_name = list(map(lambda r: r.base_name, available_rules))

parser = argparse.ArgumentParser()
# plotting generic flags
parser.add_argument(
    "--run",
    choices=available_rules_name,
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


def main():
    global available_rules, available_rules_name
    args = parser.parse_args()

    if args.list_rules:
        print("\n".join(available_rules_name))
        return
    if args.run:
        available_rules = filter(lambda x: x.base_name == args.run, available_rules)

    def process_one_transaction(entry: FireflyTransactionDataClass):

        try:
            for rule in available_rules:
                if rule.base_name in args.disable:
                    continue
                rule.process(entry)
        except rules.base_rule.StopRuleProcessing:
            pass

    start = dateutil_parser("1 Jan 2000").date()
    end = dateutil_parser("1 Jan 2200").date()
    for data in get_transactions(start, end):
        process_one_transaction(data)

    print("========================")

    for acc, updates_in_one_acc in group_by(
        pending_updates.values(), lambda x: x.acc
    ).items():
        print(f"{acc}:")
        grouped_rule_updates = group_by(updates_in_one_acc, lambda x: x.rule)
        for rule_name, updates_in_one_rule in grouped_rule_updates.items():

            print(f" >> rule: {rule_name} <<")
            for updates in sorted(updates_in_one_rule, key=lambda x: x.date):
                print(updates)

                # updates.apply()

    if len(pending_updates) == 0:
        print("No update necessary.")
        exit()

    print("=========================")
    user_input = input(
        ">> IMPORTANT: Review the above output and see if the updates are ok: [yN]"
    )
    if user_input.lower() != "y":
        print("Aborting...")
        exit(1)

    for updates in tqdm.tqdm(pending_updates.values(), desc="Applying updates"):
        updates.apply(dry_run=False)


if __name__ == "__main__":
    main()
