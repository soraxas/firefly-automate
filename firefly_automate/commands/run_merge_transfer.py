#!/bin/env python
import argparse
import logging
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd
import pytz
import tqdm

from firefly_automate.firefly_request_manager import (
    get_merge_as_transfer_rule_id,
    send_transaction_delete,
    update_rule_action,
)
from firefly_automate.miscs import PendingUpdates

LOGGER = logging.getLogger()


@dataclass
class MergingRequest:
    info_df: pd.DataFrame
    destination_acc_name: str
    withdrawl_to_transfer_update: PendingUpdates
    deposit_transaction_to_delete: str


command_name = "merge"


def init_subparser(parser):
    parser.add_argument(
        "-d",
        "--max-days-differences",
        help=(
            "The maximum days differences between 2 transactions that have same $ amount "
            "to classify them as an transfer between personal accounts."
        ),
        default=0,
        type=int,
    )
    parser.add_argument(
        "--max-amount-differences",
        help=(
            "The maximum amount differences between 2 transactions "
            "to classify them as an transfer between personal accounts."
        ),
        default=1e-4,
        type=float,
    )
    parser.add_argument(
        "--batch-size",
        help="The batch size to confirm the pending transfer merges from user.",
        default=5,
        type=int,
    )


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

    # pending_deletes.clear()


def print_df(df: pd.DataFrame):
    print(df.fillna("").to_markdown(index=False, floatfmt=".2f"))


def run(args: argparse.ArgumentParser):
    all_transactions = args.get_transactions()

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
    df["date"] = pd.to_datetime(
        df["date"],
        utc=True,
        # infer_datetime_format=True,
    )

    withdrawal = df[df["type"] == "withdrawal"]
    deposit = df[df["type"] == "deposit"]

    amount_different = np.abs(
        np.asarray(withdrawal["amount"].astype(float))[:, np.newaxis]
        - np.asarray(deposit["amount"].astype(float))
    )

    PENDING_DELETE_ID = set()

    process_Q = []
    for withdrawal_idx in range(amount_different.shape[0]):
        # potential match based on date being similar
        potential_match_deposit_indices = np.where(
            amount_different[withdrawal_idx, :] <= args.max_amount_differences
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
                withdrawal_deposit_pair_diff <= args.max_days_differences
            ]

            # remove matches that are fom the same account
            potential_match_by_date = potential_match_by_date[
                potential_match_by_date.dest != withdrawal.iloc[withdrawal_idx].source
            ]

            # remove any matches that had already been deleted
            potential_match_by_date = potential_match_by_date[
                ~potential_match_by_date.id.isin(PENDING_DELETE_ID)
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

                if len(process_Q) >= args.batch_size:
                    process_in_batch(process_Q)
                    process_Q.clear()

                PENDING_DELETE_ID.add(canidate_transfer_to.id)
    process_in_batch(process_Q)
