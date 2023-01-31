#!/bin/env python
import argparse
import logging
import sys
from typing import List, Iterable, Any

import argcomplete
import firefly_iii_client
import pandas as pd
import tqdm
from firefly_iii_client.api import accounts_api
from firefly_iii_client.model.transaction_split_store import TransactionSplitStore
from firefly_iii_client.model.transaction_store import TransactionStore
from firefly_iii_client.model.transaction_type_property import TransactionTypeProperty
from icecream import ic

from firefly_automate.connections_helpers import (
    extract_data_from_pager,
    FireflyPagerWrapper,
)
from firefly_automate.firefly_request_manager import (
    get_firefly_client_conf,
    send_transaction_create,
)
from firefly_automate.miscs import (
    group_by,
)
from firefly_automate.miscs import setup_logger

LOGGER = logging.getLogger()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--yes",
    action="store_true",
    help="Assume yes to all confirmations",
)
parser.add_argument(
    "file_input",
    type=str,
    nargs="?",
    help="target csv file (or read from stdin)",
    default=sys.stdin,
)
parser.add_argument(
    "-s",
    "--skip-rows",
    help="skip rows",
    type=int,
    default=0,
)
parser.add_argument(
    "-d", "--drop", help="drop columns", default=[], nargs="+", type=str
)
parser.add_argument(
    "-t",
    "--as-datetime",
    help="columns to interpret as datetime format",
    default=[],
    nargs="+",
    type=str,
)
parser.add_argument(
    "-f",
    "--as-float",
    help="columns to interpret as float",
    default=[],
    nargs="+",
    type=str,
)
parser.add_argument(
    "-i",
    "--no-interpret-int-as-column",
    help="interpret integer as column index",
    default=True,
    dest="interpret_int_as_column",
    action="store_false",
)
parser.add_argument(
    "--source-name",
    help="source account name",
    type=str,
)
parser.add_argument(
    "--destination-name",
    help="destination account name",
    type=str,
)

parser.add_argument(
    "--debug",
    default=False,
    help="Debug logging",
    action="store_true",
)
argcomplete.autocomplete(parser)


def transform_col_index_to_name(df: pd.DataFrame, columns: List[str]) -> Iterable[str]:
    for col in columns:
        try:
            col = int(col)
        except ValueError:
            yield col
        else:
            # treat int as column index; return column name
            yield df.columns[col]


def filter_clean_dollar_format(value: str) -> str:
    """Make column with $100.00 or $1,234.00 works"""
    return str(float(value.lstrip("$").replace(",", "")))


def filter_map_input_to_transaction_store_input(key: str, value: Any):
    """Map the input into correct type."""
    if key == "type":
        return TransactionTypeProperty(value)
    else:
        return value  # default


def main():
    args = parser.parse_args()

    setup_logger(args.debug)
    if not args.debug:
        ic.disable()

    df = pd.read_csv(args.file_input, skiprows=args.skip_rows)

    if args.interpret_int_as_column:
        args.drop = list(transform_col_index_to_name(df, args.drop))
        args.as_datetime = list(transform_col_index_to_name(df, args.as_datetime))
        args.as_float = list(transform_col_index_to_name(df, args.as_float))

    if len(args.drop) > 0:
        df = df.drop(columns=list(args.drop))
    for col in args.as_datetime:
        df[col] = pd.to_datetime(df[col], infer_datetime_format=True)

    for col in args.as_float:
        df[col] = df[col].apply(filter_clean_dollar_format)

    with pd.option_context(
        "display.max_columns", None, "display.max_colwidth", 20, "display.width", 0
    ):
        print(df)
        print(df.info(verbose=True))

    # pprint.pprint(get_firefly_account_mappings())

    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        api_instance = accounts_api.AccountsApi(api_client)

        all_accs = group_by(
            extract_data_from_pager(
                FireflyPagerWrapper(api_instance.list_account, "accounts")
            ),
            functor=lambda x: x["attributes"]["type"],
        )

        with pd.option_context(
            "display.max_columns",
            None,
            "display.max_rows",
            None,
            "display.max_colwidth",
            0,
            "display.width",
            0,
        ):
            print(df)
            print(df.info(verbose=True))

            data = []
            for k, grouped_acc in all_accs.items():
                for _acc in grouped_acc:
                    data.append([_acc["id"], k, _acc["attributes"]["name"]])

            print(
                pd.DataFrame(
                    data,
                    columns=[
                        "id",
                        "type",
                        "Acc name",
                    ],
                ).to_markdown(index=False)
            )

    new_transactions = []
    for index, row in df.iterrows():
        post_data = dict()
        if row["type"] == "withdrawal":
            assert args.source_name
            post_data["source_name"] = args.source_name
        if row["type"] == "deposit":
            assert args.destination_name
            post_data["destination_name"] = args.destination_name
        post_data.update(
            {
                k: filter_map_input_to_transaction_store_input(k, v)
                for k, v in row.to_dict().items()
            }
        )

        new_transaction = TransactionStore(
            apply_rules=True,
            transactions=[
                TransactionSplitStore(**post_data),
            ],
        )
        print(new_transaction)
        new_transactions.append(new_transaction)

    # money patch to force capturing input terminal, instead of potential pipe
    sys.stdin = open("/dev/tty")
    if input("Looks Good?: [y/N]").lower() == "y":
        for new_transaction in tqdm.tqdm(new_transactions):
            send_transaction_create(new_transaction)


if __name__ == "__main__":
    main()
