#!/bin/env python
import os
import logging
import pickle
from typing import Dict, List, Set
import argparse, argcomplete

import pytz
import tqdm

import numpy as np

from dataclasses import dataclass
import pandas as pd
from datetime import datetime
from dateutil.parser import parse as dateutil_parser
from dateutil.relativedelta import relativedelta

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
    setup_logger,
)

from firefly_automate.firefly_request_manager import (
    update_rule_action,
    get_merge_as_transfer_rule_id,
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


@dataclass
class MergingRequest:
    info_df: pd.DataFrame
    destination_acc_name: str
    withdrawl_to_transfer_update: PendingUpdates
    deposit_transaction_to_delete: str


MAX_DAYS_DIFF = 1
MAX_DAYS_DIFF = 3
MAX_AMOUNT_DIFF = 1e-4
BATCH_SIZE = 5


def process_in_batch(pending_updates: List[MergingRequest]):
    if len(pending_updates) == 0:
        return

    print("========================")
    print()
    print("========================")

    def print_pending_updates(pending_updates):
        for i, pending_update in enumerate(pending_updates):
            print("<" + ("-" * 18) + f" {i+1} " + ("-" * 18) + ">")
            print_df(pending_update.info_df)
            print(f"=" * (20 * 2 + 3))

    print_pending_updates(pending_updates)
    while True:
        print(
            ">> IMPORTANT: Review the above output and see if the updates are ok. Or enter space-separated number to ignore:"
        )
        inputs = input(f">> [1-{len(pending_updates)}/y/N] ")
        inputs = inputs.strip()
        if inputs in ("n", "N", ""):
            break
        if inputs.lower() == "y":
            for updates in tqdm.tqdm(pending_updates, desc="Applying updates"):
                update_rule_action(
                    id=get_merge_as_transfer_rule_id(),
                    action_packs=[
                        (
                            "convert_transfer",
                            updates.destination_acc_name,
                        ),
                        (
                            "remove_tag",
                            "AUTOMATE_convert-as-transfer",
                        ),
                    ],
                )
                updates.withdrawl_to_transfer_update.apply(dry_run=False)

                send_transaction_delete(updates.deposit_transaction_to_delete)
            break
        else:
            try:
                nums = [int(num.strip()) - 1 for num in inputs.split(" ")]
            except ValueError:
                print("Invalid choices")
                continue
            nums = list(reversed(sorted(set(nums))))
            print(nums)
            if not all(0 <= n < len(pending_updates) for n in nums):
                print("Number out of range.")
                continue
            # remove them from pendings
            for i in nums:
                pending_updates.pop(i)
            if len(pending_updates) == 0:
                break
            print_pending_updates(pending_updates)

    pending_deletes.clear()


def print_df(df: pd.DataFrame):
    print(df.fillna("").to_markdown(index=False, floatfmt=".2f"))


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

    withdrawal = df[df["type"] == "withdrawal"]
    deposit = df[df["type"] == "deposit"]

    amount_different = np.abs(
        np.asarray(withdrawal["amount"].astype(float))[:, np.newaxis]
        - np.asarray(deposit["amount"].astype(float))
    )

    DELETED_ID = set()

    process_Q = []
    for withdrawal_idx in range(amount_different.shape[0]):
        # potential match based on date being similar
        potential_match_deposit_indices = np.where(
            amount_different[withdrawal_idx, :] <= MAX_AMOUNT_DIFF
        )[0]

        if len(potential_match_deposit_indices) > 0:

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

            # remove matches that are fom the same account
            potential_match_by_date = potential_match_by_date[
                potential_match_by_date.dest != withdrawal.iloc[withdrawal_idx].source
            ]

            # remove any matches that had already been deleted
            potential_match_by_date = potential_match_by_date[
                ~potential_match_by_date.id.isin(DELETED_ID)
            ]

            if len(potential_match_by_date) > 0:

                canidate_transfer_from = withdrawal.iloc[withdrawal_idx]

                if len(potential_match_by_date) > 1:
                    info_row = [np.nan] * (len(withdrawal.columns) - 1)
                    info_row[1] = "---Select followings---"
                    info_df = pd.DataFrame(
                        [
                            withdrawal.iloc[withdrawal_idx].values.tolist(),
                            info_row,
                            *potential_match_by_date.values.tolist(),
                        ],
                        columns=withdrawal.columns,
                    )
                    info_df["amount"] = pd.to_numeric(info_df.amount)

                    print("===============================")
                    print_df(info_df)
                    while True:
                        _id = input(
                            f"> which transaction ID do you want to merge? {potential_match_by_date.id.tolist()} "
                        )
                        _potential_match_by_date = potential_match_by_date[
                            potential_match_by_date.id == _id
                        ]
                        if len(_potential_match_by_date) == 1:
                            potential_match_by_date = _potential_match_by_date
                            break
                        print(f"Invalid selection, not was matched.")

                info_df = pd.DataFrame(
                    [
                        withdrawal.iloc[withdrawal_idx].values.tolist(),
                        *potential_match_by_date.values.tolist(),
                    ],
                    columns=withdrawal.columns,
                )

                canidate_transfer_to = potential_match_by_date.iloc[0]

                process_Q.append(
                    MergingRequest(
                        info_df=info_df,
                        destination_acc_name=canidate_transfer_to.dest,
                        withdrawl_to_transfer_update=PendingUpdates(
                            IDS_to_transaction[canidate_transfer_from.id],
                            "merging",
                            apply_rule=True,
                            updates_kwargs=dict(
                                description=f"[{canidate_transfer_from.desc}] > [{canidate_transfer_to.desc}]",
                                tags=["AUTOMATE_convert-as-transfer"],
                            ),
                        ),
                        deposit_transaction_to_delete=canidate_transfer_to.id,
                    )
                )

                if len(process_Q) >= BATCH_SIZE:
                    process_in_batch(process_Q)
                    process_Q.clear()

                DELETED_ID.add(canidate_transfer_to.id)
    process_in_batch(process_Q)


if __name__ == "__main__":
    main()
