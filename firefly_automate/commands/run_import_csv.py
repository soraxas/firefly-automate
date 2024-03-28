#!/bin/env python
import argparse
import sys
from typing import Any, Iterable, List

import firefly_iii_client
import pandas as pd
import numpy as np
import tqdm
from firefly_iii_client.api import accounts_api
from firefly_iii_client.model.transaction_split_store import TransactionSplitStore
from firefly_iii_client.model.transaction_store import TransactionStore
from firefly_iii_client.model.transaction_type_property import TransactionTypeProperty

from firefly_automate.connections_helpers import (
    FireflyPagerWrapper,
    extract_data_from_pager,
)
from firefly_automate.firefly_request_manager import (
    get_firefly_client_conf,
    send_transaction_create,
)
from firefly_automate.miscs import group_by


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
    if type(value) == float:
        return value
    return str(float(value.lstrip("$").replace(",", "")))


def filter_map_input_to_transaction_store_input(key: str, value: Any):
    """Map the input into correct type."""
    if key == "type":
        return TransactionTypeProperty(value)
    else:
        return value  # default


command_name = "import_csv"


def select_option(options, query: str = ""):
    while True:
        print(f"{query}")
        for i, o in enumerate(options):
            print(f" [{i+1}] {o}")
        choice = input(f"Select [1-{len(options)}]: ")
        print()
        try:
            choice = int(choice) - 1
        except ValueError:
            print("[ERROR] Invalid string.\n")
            continue
        if choice < 0 or choice >= len(options):
            print("[ERROR] Choice is out of range.\n")
            continue
        options = list(options)
        selected = options.pop(choice)
        return selected, options


def init_subparser(parser):
    parser.add_argument(
        "file_input",
        type=str,
        nargs="?",
        help="target csv file (or read from stdin)",
        default=None,
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
    parser.set_defaults(parser=parser)


def run(args: argparse.ArgumentParser):
    if not sys.stdin.isatty() and args.file_input is None:
        # read frome stdin
        args.file_input = sys.stdin
    elif sys.stdin.isatty() and args.file_input is None:
        print(args)
        args.parser.print_usage()
        print(f"{args.parser.prog}: there was no stdin and no csv file given.")
        exit(1)
    df = pd.read_csv(args.file_input, skiprows=args.skip_rows)

    if args.interpret_int_as_column:
        args.drop = list(transform_col_index_to_name(df, args.drop))
        args.as_datetime = list(transform_col_index_to_name(df, args.as_datetime))
        args.as_float = list(transform_col_index_to_name(df, args.as_float))

    if len(args.drop) > 0:
        df = df.drop(columns=list(args.drop))
    for col in args.as_datetime:
        df[col] = pd.to_datetime(
            df[col],
            # infer_datetime_format=True,
        )

    for col in args.as_float:
        df[col] = df[col].apply(filter_clean_dollar_format)

    new_df_data = {}
    print("==============================")
    print(" The following is your data")
    print(df.head())
    print("------------------------------")
    selected, remainings = select_option(
        df.columns, query="> which one is description?"
    )
    new_df_data["description"] = df[selected]

    selected, remainings = select_option(remainings, query="> which one is date?")
    new_df_data["date"] = pd.to_datetime(df[selected], infer_datetime_format=True)

    selected, _ = select_option(
        ["Yes", "No"], query="> do you have separated incoming/outgoing columns?"
    )

    # selected = 'Ye'
    # remainings = df.columns

    if selected == "No":
        raise NotImplementedError("")
    else:
        transaction_type = pd.Series([np.nan] * len(df))
        amount = pd.Series([np.nan] * len(df))
        selected, remainings = select_option(
            remainings, query="> which one is incoming (i.e. +'ve balance / Credit)?"
        )

        transaction_type[~df[selected].isna()] = "deposit"
        amount[~df[selected].isna()] = df[selected].apply(filter_clean_dollar_format)

        selected, remainings = select_option(
            remainings, query="> which one is outgoing (i.e. -'ve balance / Debit)?"
        )
        transaction_type[~df[selected].isna()] = "withdrawal"
        amount[~df[selected].isna()] = df[selected].apply(filter_clean_dollar_format)

        new_df_data["type"] = transaction_type
        new_df_data["amount"] = amount

    df = pd.DataFrame(new_df_data)

    with pd.option_context(
        "display.max_columns", None, "display.max_colwidth", 20, "display.width", 0
    ):
        print(df.to_markdown(index=False))

    if input("Looks Good?: [y/N] ").lower() != "y":
        exit(1)

    if df.amount.isnull().values.any() or df.type.isnull().values.any():
        print("[ERROR] There exist nans inside amount columns. (unable to map?)")
        exit(1)

    _zero_amount_transactions = df[df.amount.astype(float) == 0]
    if len(_zero_amount_transactions) > 0:
        print(
            "> The following row(s) will be dropped because they have $0 dollars and are not allowed in firefly-iii."
        )
        print(_zero_amount_transactions)
        if input("Proceed?: [y/N] ").lower() != "y":
            exit(1)
        for index, row in df.iterrows():
            if float(row.amount) == 0:
                df.drop(index, inplace=True)

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
                if k not in ["revenue", "expense"]:
                    for _acc in grouped_acc:
                        data.append([_acc["id"], k, _acc["attributes"]["name"]])

            bank_info_df = pd.DataFrame(
                data,
                columns=[
                    "id",
                    "type",
                    "Acc name",
                ],
            )
            print(bank_info_df.to_markdown(index=False))

    bank_id = str(int(input("> what is the id of this bank account? ")))

    bank_names = bank_info_df[bank_info_df.id == bank_id]["Acc name"]
    if len(bank_names) != 1:
        print(f"> invalid choice. Result is {bank_names}")
    bank_name = bank_names.iloc[0]
    if args.source_name is None:
        args.source_name = bank_name
    if args.destination_name is None:
        args.destination_name = bank_name

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
    if input("Looks Good?: [y/N] ").lower() == "y":
        for new_transaction in tqdm.tqdm(new_transactions):
            send_transaction_create(new_transaction)
