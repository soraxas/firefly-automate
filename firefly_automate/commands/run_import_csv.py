#!/bin/env python
import argparse
import sys
from pathlib import Path
from typing import Any, Iterable, List, Tuple

import re
import pandas as pd
import tqdm
import yaml

from firefly_automate.firefly_request_manager import (
    create_transaction_store,
    get_firefly_account_grouped_by_type,
    send_transaction_store,
)
from firefly_automate.miscs import Inequality, select_option, to_datetime


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
    if type(value) is float:
        return value
    return str(float(value.lstrip("$").replace(",", "")))


def filter_map_input_to_transaction_store_input(key: str, value: Any):
    """Map the input into correct type."""
    if key == "type":
        from firefly_iii_client.model import transaction_type_property

        return transaction_type_property.TransactionTypeProperty(value)
    else:
        return value  # default


def string_sep_with_equal_sign(arg):
    x = arg.split("=")
    assert len(x) == 2
    return x


command_name = "import_csv"

optional_attributes = [
    "currency_code",
    "foreign_amount",
    "category_name",
    "currency_code",
    "source_name",
    "destination_name",
    "bill_name",
    "tags",
    "notes",
    "internal_reference",
    "external_id",
    "process_date",
    "due_date",
    "payment_date",
    "invoice_date",
]


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
        "-i",
        "--no-interpret-int-as-column",
        help="interpret integer as column index",
        default=True,
        dest="interpret_int_as_column",
        action="store_false",
    )
    parser.add_argument(
        "--date-format",
        help="date format for parsing",
        default=None,
        type=str,
    )
    parser.add_argument(
        "--target-account-name",
        default=None,
        type=str,
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
        "-f",
        "--filter-by-col",
        help="filter rows via column with string",
        default=[],
        nargs="+",
        type=string_sep_with_equal_sign,
    )
    parser.add_argument(
        "-fd",
        "--filter-by-datetime",
        help="filter rows via column with datetime",
        default=[],
        nargs="+",
        type=Inequality.parse,
    )
    parser.add_argument(
        "-nn",
        "--non-null-by-col",
        help="filter column with non-null value",
        default=[],
        nargs="+",
        type=str,
    )
    parser.add_argument(
        "--load-mappings",
        help="load mappings from file",
        default=None,
        type=str,
    )
    parser.set_defaults(parser=parser)


def ask_for_account_name():
    all_accs = get_firefly_account_grouped_by_type()

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

    account_names = bank_info_df[bank_info_df.id == bank_id]["Acc name"]
    if len(account_names) != 1:
        print(f"> invalid choice. Result is {account_names}")
    account_name = account_names.iloc[0]
    return account_name


def manual_mapping(df):
    new_df_data = {}
    print("==============================")
    print(" The following is your data")
    print(df.head())
    print("------------------------------")
    selected, remaining = select_option(
        df.columns, query_prompt="> which one is description?"
    )
    new_df_data["description"] = df[selected]

    selected, remaining = select_option(remaining, query_prompt="> which one is date?")
    new_df_data["date"] = to_datetime(df[selected])

    selected, _ = select_option(
        ["Yes", "No"], query_prompt="> do you have separated incoming/outgoing columns?"
    )

    # selected = 'Ye'
    # remaining = df.columns

    if selected == "No":
        raise NotImplementedError("")
    else:
        transaction_type = pd.Series([None] * len(df))
        amount = pd.Series([None] * len(df))
        selected, remaining = select_option(
            remaining,
            query_prompt="> which one is incoming (i.e. +'ve balance / Credit)?",
        )

        transaction_type[~df[selected].isna()] = "deposit"
        amount[~df[selected].isna()] = df[selected].apply(filter_clean_dollar_format)

        selected, remaining = select_option(
            remaining,
            query_prompt="> which one is outgoing (i.e. -'ve balance / Debit)?",
        )
        transaction_type[~df[selected].isna()] = "withdrawal"
        amount[~df[selected].isna()] = df[selected].apply(filter_clean_dollar_format)

        new_df_data["type"] = transaction_type
        new_df_data["amount"] = amount

    return pd.DataFrame(new_df_data)


im_source = lambda _d: _d.type == "withdrawal"
im_destination = lambda _d: _d.type == "deposit"


def auto_mapping(df, preset_mappings):
    new_df_data = pd.DataFrame()

    def __split_by_colon(_str, nparts: int = 3):
        # this regex split on colon that are not escaped by backslash \
        _parts = re.split(r"(?<!\\):", _str)

        assert len(_parts) == nparts, f"must have {nparts} parts separated by colon"
        # replace any escaped colon back to its original form
        return [p.replace(r"\:", ":") for p in _parts]

    def _assign_col(map_to_col, data, override: bool = False):
        if override or map_to_col not in new_df_data:
            new_df_data[map_to_col] = data
            return
        if map_to_col == "tag":
            raise NotImplementedError(
                "Multi-Tag is not implemented (they will override each other right now.)"
            )
        new_df_data[map_to_col] += data

    def _process_special(source, extract_as):
        if extract_as.startswith("__auto-abs:"):
            _, map_to_col = __split_by_colon(extract_as, 2)
            _assign_col(map_to_col, df[source].astype(float).abs().astype(str))

        elif extract_as.startswith("__strip:"):
            _, map_to_col = __split_by_colon(extract_as, 2)
            _assign_col(map_to_col, df[source].str.strip("'\"\n\t "))

        elif extract_as.startswith("__auto-type:"):
            _, _conf, map_to_col = __split_by_colon(extract_as, 3)
            withdrawal, deposit = _conf.split("-")

            # map from the specified keyword into either withdrawal or deposit
            _series = pd.Series([None] * len(df), dtype=object)
            _series[df[source] == withdrawal] = "withdrawal"
            _series[df[source] == deposit] = "deposit"
            _assign_col(map_to_col, _series)

        elif extract_as.startswith("__auto-pos-neg:"):
            _, _conf, map_to_col = __split_by_colon(extract_as, 3)
            pos, neg = _conf.split("-")

            # assign based on whether this column is positive or negative
            __col_as_float = df[source].astype(float)
            _series = pd.Series([None] * len(df), dtype=object)
            _series[__col_as_float > 0] = pos
            _series[__col_as_float < 0] = neg
            _assign_col(map_to_col, _series)

        elif extract_as.startswith("__auto-source-destination__"):
            # assign based on whether this is a source acc or destination acc
            _attr = extract_as[len("__auto-source-destination__") :]
            new_df_data.loc[_transactions_as_source, f"source_{_attr}"] = df[source]
            new_df_data.loc[_transactions_as_destination, f"destination_{_attr}"] = df[
                source
            ]

        elif extract_as.startswith("__auto-inv-source-destination__"):
            # assign based on whether this is a source acc or destination acc
            _attr = extract_as[len("__auto-inv-source-destination__") :]
            new_df_data.loc[_transactions_as_destination, f"source_{_attr}"] = df[
                source
            ]
            new_df_data.loc[_transactions_as_source, f"destination_{_attr}"] = df[
                source
            ]

        elif extract_as.startswith("__py-expr:"):
            _, _expression, map_to_col = __split_by_colon(extract_as, 3)

            # DANGEROUS of eval any arbitary expression. You need to trust your own config.
            def __run(x: pd.Series):
                return eval(_expression)

            _assign_col(map_to_col, __run(df[source]))

        elif extract_as.startswith("__"):
            raise NotImplementedError(
                f"Detected special rule '{extract_as}', but I don't "
                "know how to handle that."
            )
        else:
            return False
        return True

    mapping_pairs: List[Tuple[str, str]] = []
    for k, v in preset_mappings.items():
        # make it so that we supports using list in a key-val reference.
        if not isinstance(v, list):
            v = [v]
        mapping_pairs.extend([(k, _v) for _v in v])
    del preset_mappings

    def process_type_first(_mapping_pairs):
        # very very first, process the type of transaction
        # that's because many rules depends on knowing if a given transaction is source or dest first.
        new_mapping_pairs = []
        for k, v in _mapping_pairs:
            if v.endswith("type"):
                _process_special(k, v)
            else:
                new_mapping_pairs.append((k, v))
        return new_mapping_pairs

    mapping_pairs = process_type_first(mapping_pairs)
    _transactions_as_source = im_source(new_df_data)  # keep track for future reference
    _transactions_as_destination = im_destination(
        new_df_data
    )  # keep track for future reference

    # first process all the special ones
    for k, v in mapping_pairs:
        print(k, v)
        if not _process_special(k, v):  # True == processed
            # _remaining_mappings[k] = v
            _assign_col(v, df[k])
        if v.endswith("date"):
            _assign_col(v, to_datetime(new_df_data[v]), override=True)

    print("==================================")
    print(" The following is your mapped data")
    print(df.head())
    print("---------------------------------")
    return new_df_data


def run(args: argparse.Namespace):
    if not sys.stdin.isatty() and args.file_input is None:
        # read from stdin
        args.file_input = sys.stdin
    elif sys.stdin.isatty() and args.file_input is None:
        args.parser.print_usage()
        print(f"{args.parser.prog}: there was no stdin and no csv file given.")
        exit(1)
    df = pd.read_csv(args.file_input, skiprows=args.skip_rows, dtype=object)

    if args.interpret_int_as_column:
        args.drop = list(transform_col_index_to_name(df, args.drop))
        args.as_datetime = list(transform_col_index_to_name(df, args.as_datetime))
        # args.as_float = list(transform_col_index_to_name(df, args.as_float))

    ############################################################
    args.column_mappings = None

    if args.load_mappings:
        mappings = yaml.safe_load(Path(args.load_mappings).read_text())
        # see if there's a base setting. if so, merge them.
        _base_mapping_fname = mappings.pop("__base_setting_yaml", None)
        if _base_mapping_fname:
            _base_mapping = yaml.safe_load(Path(_base_mapping_fname).read_text())
            # merge with base
            for k, v in _base_mapping.items():
                if k not in mappings:
                    mappings[k] = v

        args.column_mappings = mappings.pop("column_mappings", None)

        if "filter_by_col" in mappings:
            args.filter_by_col.extend(
                string_sep_with_equal_sign(x) for x in mappings.pop("filter_by_col")
            )
        if "non_null_by_col" in mappings:
            args.non_null_by_col.extend(mappings.pop("non_null_by_col"))

        allowed_mappings = {
            "target_account_name",
            "date_format",
            "date_format_day_first",
        }

        for key in mappings:
            if key not in allowed_mappings:
                print(
                    f">> There are unused mappings in the given '{args.load_mappings}' file.\n"
                    f">> Unused keys: {key}.\n"
                    f">> Including keys: {list(mappings.keys())}.\n"
                    f">> Full content: {mappings}.\n"
                )
                exit(1)
            # assign to args for later access
            setattr(args, key, mappings[key])

    ############################################################

    if len(args.drop) > 0:
        df = df.drop(columns=list(args.drop))
    for col, val in args.filter_by_col:
        df = df[df[col] == val]
    for col in args.non_null_by_col:
        df = df[~df[col].isna()]
    for col in args.as_datetime:
        df[col] = to_datetime(df[col])
    for col, inequality_sign, datetime_val in args.filter_by_datetime:
        from dateutil.parser import parse as dateutil_parser

        try:
            _datetime_col = to_datetime(df[col]).dt.date
        except:
            print(f"> exception during time parsing with col {col}")
        datetime_val = dateutil_parser(datetime_val, dayfirst=True).date()

        mask = Inequality.compare(_datetime_col, inequality_sign, datetime_val)
        df = df[mask]

    print("------------------------------")
    print(df.head())
    print("------------------------------")
    if len(df) <= 0:
        return

    # reset index for any filtered values
    df = df.reset_index()

    if args.column_mappings is None:
        df = manual_mapping(df)
    else:
        df = auto_mapping(df, args.column_mappings)

    # sort by date
    df.sort_values(by="date", inplace=True)

    account_name = (
        args.target_account_name if hasattr(args, "target_account_name") else None
    )
    if account_name is None:
        account_name = ask_for_account_name()
    if args.source_name is None:
        args.source_name = account_name
    if args.destination_name is None:
        args.destination_name = account_name

    df.loc[im_destination(df), "destination_name"] = account_name
    df.loc[im_source(df), "source_name"] = account_name

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
            "> The following row(s) will be dropped because they have $0 dollars "
            "and are not allowed in firefly-iii."
        )
        print(_zero_amount_transactions)
        if input("Proceed?: [y/N] ").lower() != "y":
            exit(1)
        for index, row in df.iterrows():
            if float(row.amount) == 0:
                df.drop(index, inplace=True)

    # pprint.pprint(get_firefly_account_mappings())

    new_transactions = []
    for index, row in df.iterrows():
        new_transaction = create_transaction_store(
            transaction_data={
                k: filter_map_input_to_transaction_store_input(k, v)
                for k, v in row.to_dict().items()
            },
            apply_rules=True,
        )
        print(new_transaction)
        new_transactions.append(new_transaction)

    # money patch to force capturing input terminal, instead of potential pipe
    sys.stdin = open("/dev/tty")
    if input("Looks Good?: [y/N] ").lower() == "y":
        for new_transaction in tqdm.tqdm(new_transactions):
            send_transaction_store(new_transaction)
