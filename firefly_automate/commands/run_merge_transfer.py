#!/bin/env python
import argparse
import atexit
import logging
from dataclasses import dataclass
from multiprocessing import Lock
from typing import List, Set, Tuple

import numpy as np
import pandas as pd
import pytz
import tqdm

from firefly_automate.config_loader import config
from firefly_automate.data_type.pending_update import (
    PendingUpdates,
)


@dataclass
class MergingRequest:
    info_df: pd.DataFrame
    destination_acc_name: str
    withdrawl_to_transfer_update: PendingUpdates
    deposit_transaction_to_delete: str

    def get_ids(self):
        return tuple(sorted(int(_id) for _id in self.info_df.id.values))


conf = config["merge_transfer"]

IGNORED_IDS: Set[Tuple[int]] = {tuple(sorted(pair)) for pair in conf["ignore_id_pairs"]}

PENDING_IGNORED_MERGE_REQUEST: List[MergingRequest] = []


from firefly_automate.connections_helpers import AsyncRequest, ignore_keyboard_interrupt
from firefly_automate.data_type.pending_update import PendingUpdates
from firefly_automate.firefly_request_manager import (
    get_merge_as_transfer_rule_id,
    send_transaction_delete,
    update_rule_action,
)
from firefly_automate.miscs import to_datetime

LOGGER = logging.getLogger()


# this is a lock to make sure that the rule trigger and tagging operation is an atomic operation
RULE_AND_TAGGING_LOCK = Lock()


def merge_atomic_operation(_transfer_update, dest_acc_name: str):
    with RULE_AND_TAGGING_LOCK:
        # set the rule to auto convert tagged transaction to this destination
        update_rule_action(
            id=get_merge_as_transfer_rule_id(),
            action_packs=[
                (
                    "convert_transfer",
                    dest_acc_name,
                ),
                (
                    "remove_tag",
                    "AUTOMATE_convert-as-transfer",
                ),
            ],
        )
        # now let's add this new tag to the withdrawal
        _transfer_update.apply(dry_run=False)


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
        default=3,
        type=int,
    )


REGISTERED_ON_EXIT_STATUS = False


# define a function that keep track of successful merge requests
def _on_exit_status(_queue):
    # wait for all async process to finish
    successes = []
    for p in tqdm.tqdm(_queue, desc="Waiting for updates to finish in background."):
        ignore_keyboard_interrupt(
            lambda: successes.append(p.get()),
            reason="waiting to show current background jobs status.",
        )
    print(f"Successes: {sum(s for s in successes if s is True)}/{len(_queue)}")


def process_in_batch(pending_updates: List[MergingRequest], _async_process_Q: List):
    if len(pending_updates) == 0:
        return

    print("^^^^^^^^^^^^^^^^^^^^^^^^")
    print()
    print("vvvvvvvvvvvvvvvvvvvvvvvv")

    def print_pending_updates(pending_updates):
        for i, pending_update in enumerate(pending_updates):
            print("<" + ("-" * 18) + f" {i+1} " + ("-" * 18) + ">")
            print_df(pending_update.info_df)
            print(f"=" * (20 * 2 + 3))

    while len(pending_updates) > 0:
        print_pending_updates(pending_updates)
        print(
            ">> IMPORTANT: Review the above output and see if the updates are ok. Or enter space-separated number to ignore:"
        )
        inputs = input(f">> [1-{len(pending_updates)}/y/N] ")
        inputs = inputs.strip().lower()
        if inputs in ("n", ""):
            for u in pending_updates:
                PENDING_IGNORED_MERGE_REQUEST.append(u)
            break
        if inputs == "y":
            # send to run in background.
            def runner(_updates):
                # for updates in tqdm.tqdm(pending_updates, desc="Applying updates"):
                for updates in _updates:
                    merge_atomic_operation(
                        updates.withdrawl_to_transfer_update,
                        dest_acc_name=updates.destination_acc_name,
                    )
                    # and delete the corresponding deposit event
                    send_transaction_delete(updates.deposit_transaction_to_delete)
                return True

            _async_process_Q.append(
                AsyncRequest.run(runner, _updates=list(pending_updates))
            )
            global REGISTERED_ON_EXIT_STATUS
            if not REGISTERED_ON_EXIT_STATUS:
                # register an on-exit status on-demand (so that it will be processed first via FILO)
                REGISTERED_ON_EXIT_STATUS = True
                atexit.register(_on_exit_status, _queue=_async_process_Q)
            break
        else:
            try:
                nums = [int(num.strip()) - 1 for num in inputs.split(" ")]
            except ValueError:
                print("Invalid choices")
                continue
            nums = list(reversed(sorted(set(nums))))
            if not all(0 <= n < len(pending_updates) for n in nums):
                print("Number out of range.")
                continue
            # remove them from pendings
            for i in nums:
                ignored = pending_updates.pop(i)
                PENDING_IGNORED_MERGE_REQUEST.append(ignored)
    return True

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
    df["date"] = to_datetime(
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

    async_process_Q = []

    process_batch = []
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

                merge_request = MergingRequest(
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

                if merge_request.get_ids() in IGNORED_IDS:
                    continue

                process_batch.append(merge_request)
                PENDING_DELETE_ID.add(canidate_transfer_to.id)

                if len(process_batch) >= args.batch_size:
                    process_in_batch(process_batch, async_process_Q)
                    process_batch.clear()

    process_in_batch(process_batch, async_process_Q)
    process_batch.clear()

    if len(PENDING_IGNORED_MERGE_REQUEST) > 0:
        print("=" * 20)
        print("> The following ids are ignored:")
        for req in PENDING_IGNORED_MERGE_REQUEST:
            desc = req.info_df.iloc[0].desc[:20]
            trans1 = f"{req.info_df.iloc[0].source} => {req.info_df.iloc[0].dest}"
            trans2 = f"{req.info_df.iloc[1].source} => {req.info_df.iloc[1].dest}"
            print(f"    - {list(req.get_ids())}  # {desc} [{trans1}] ~ [{trans2}]")
