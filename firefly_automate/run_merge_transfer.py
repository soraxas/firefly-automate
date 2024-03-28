#!/bin/env python
import argparse
import logging
import os
import pickle
from datetime import datetime
from typing import Dict, Set

import argcomplete
import pandas as pd
import pytz
import tqdm
from dateutil.parser import parse as dateutil_parser
from dateutil.relativedelta import relativedelta

import firefly_automate.rules.base_rule
from firefly_automate import rules
from firefly_automate.firefly_request_manager import (
    get_transactions,
    send_transaction_delete,
)
from firefly_automate.miscs import (
    FireflyTransactionDataClass,
    PendingUpdates,
    group_by,
    prompt_response,
    setup_logger,
)

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


def main():
    global available_rules
    args = parser.parse_args()

    if args.list_rules:
        print("\n".join(all_rules_name))
        return
    if args.run:
        available_rules = list(filter(lambda x: x.base_name == args.run, all_rules))
    setup_logger(args.debug)

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

    from icecream import ic

    IDS_to_transaction = {t.id: t for t in all_transactions}

    df = pd.DataFrame(
        [
            [
                t.type,
                t.date,
                t.id,
                t.description,
                t.amount,
                t.source_name,
                t.destination_name,
            ]
            for t in all_transactions
        ],
        columns=["type", "date", "id", "desc", "amount", "source", "dest"],
    )

    df["date"] = pd.to_datetime(df["date"], infer_datetime_format=True)

    import numpy as np

    ic(all_transactions[-2:])

    ic(df)

    withdrawal = df[df["type"] == "withdrawal"]
    deposit = df[df["type"] == "deposit"]

    ic(withdrawal)
    ic(deposit)

    MAX_DAYS_DIFF = 1
    MAX_DAYS_DIFF = 0
    MAX_AMOUNT_DIFF = 1e-4

    amount_different = np.abs(
        np.asarray(withdrawal["amount"].astype(float))[:, np.newaxis]
        - np.asarray(deposit["amount"].astype(float))
    )

    ic(amount_different)

    out = np.asarray(withdrawal["date"])[:, None] - np.asarray(deposit["date"])
    # days_different = np.abs(np.vectorize(lambda x: x.days)(out))

    print(out.shape)
    ic(out.dtype)

    all_potential_match = dict()

    for withdrawal_idx in range(amount_different.shape[0]):
        # potential match based on date being similar
        potential_match_deposit_indices = np.where(
            amount_different[withdrawal_idx, :] <= MAX_AMOUNT_DIFF
        )[0]

        if len(potential_match_deposit_indices) > 0:
            import pytz

            withdrawal_date = withdrawal.iloc[withdrawal_idx]["date"].astimezone(
                tz=pytz.UTC
            )
            deposit_dates = deposit.iloc[potential_match_deposit_indices]["date"].apply(
                lambda x: x.astimezone(tz=pytz.UTC)
            )

            withdrawal_deposit_pair_diff = np.abs(
                withdrawal_date - deposit_dates
            ).apply(lambda x: x.days)
            potential_match_by_date = deposit.iloc[potential_match_deposit_indices][
                withdrawal_deposit_pair_diff <= MAX_DAYS_DIFF
            ]

            # ic(potential_match_by_date)
            if len(potential_match_by_date) > 0:
                out = np.asarray(withdrawal["date"])[:, None] - np.asarray(
                    deposit["date"]
                )
                days_different = np.abs(np.vectorize(lambda x: x.days)(out))

                all_potential_match[withdrawal_idx] = potential_match_deposit_indices
                # ic(withdrawal_idx, potential_match_deposit_indices)
                #
                # ic(withdrawal.iloc[withdrawal_idx]['date'])
                # ic(deposit.iloc[potential_match_by_date])

                print("===============================")
                print(withdrawal.iloc[withdrawal_idx].to_frame().T.to_markdown())
                print(potential_match_by_date.to_markdown())

                print(potential_match_by_date.iloc[0].dest)

                canidate_transfer_from = withdrawal.iloc[withdrawal_idx]
                canidate_transfer_to = potential_match_by_date.iloc[0]

                from firefly_automate.firefly_request_manager import (
                    get_merge_as_transfer_rule_id,
                    update_rule_action,
                )

                update_rule_action(
                    id=get_merge_as_transfer_rule_id(),
                    action_packs=[
                        (
                            "convert_transfer",
                            canidate_transfer_to.dest,
                        ),
                        (
                            "remove_tag",
                            "AUTOMATE_convert-as-transfer",
                        ),
                    ],
                )

                pending_updates = [
                    PendingUpdates(
                        IDS_to_transaction[canidate_transfer_from.id],
                        "merging",
                        apply_rule=True,
                        updates_kwargs=dict(
                            description=f"[{canidate_transfer_from.desc}] > [{canidate_transfer_to.desc}]",
                            tags=["AUTOMATE_convert-as-transfer"],
                        ),
                    ),
                    # PendingUpdates(
                    #     IDS_to_transaction[canidate_transfer_to.id],
                    #     "merging",
                    #     updates_kwargs=dict(
                    #         tags=["AUTOMATE_delete"]
                    #     ),
                    # ),
                ]

                print("========================")
                for pending_update in pending_updates:
                    print(pending_update)

                if prompt_response(
                    ">> IMPORTANT: Review the above output and see if the updates are ok:"
                ):
                    for updates in tqdm.tqdm(pending_updates, desc="Applying updates"):
                        updates.apply(dry_run=False)

                    send_transaction_delete(canidate_transfer_to.id)

                # print()
                # exit()

                # count += 1
                # if count > 20:
                #     exit()

    ic(all_potential_match)
    # print(withdrawal['date'] - deposit['date'])

    exit()

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
        if args.yes or prompt_response(
            ">> IMPORTANT: Review the above output and see if the updates are ok:"
        ):
            for updates in tqdm.tqdm(pending_updates.values(), desc="Applying updates"):
                updates.apply(dry_run=False)

    elif len(pending_deletes) > 0:
        if args.yes or prompt_response(">> Ready to perform the delete?"):
            for deletes_id in tqdm.tqdm(pending_deletes, desc="Applying deletes"):
                send_transaction_delete(int(deletes_id))


if __name__ == "__main__":
    main()
