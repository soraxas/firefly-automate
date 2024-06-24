import argparse
import logging
import pprint
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Match,
    Optional,
    TypeVar,
    Union,
)

import pandas as pd

from datetime import datetime
from dateutil.parser import parse as dateutil_parser
from firefly_automate import firefly_request_manager

if TYPE_CHECKING:
    from firefly_automate.data_type.pending_update import TransactionOwnerReturnType
    from firefly_automate.data_type.transaction_type import FireflyTransactionDataClass


def get_transaction_owner(
    entry: "FireflyTransactionDataClass",
    actual_name: bool = False,
) -> "TransactionOwnerReturnType":
    belongs_to: "TransactionOwnerReturnType"
    acc_id_to_name = firefly_request_manager.get_firefly_account_mappings()
    if entry.type == "withdrawal":
        belongs_to = entry.source_id
    elif entry.type == "deposit":
        belongs_to = entry.destination_id
    elif entry.type == "transfer":
        belongs_to = (entry.source_id, entry.destination_id)
    elif entry.type == "opening balance":
        belongs_to = entry.destination_id
    else:
        raise ValueError(entry.type)
    if actual_name:
        if type(belongs_to) is tuple:
            return acc_id_to_name[belongs_to[0]], acc_id_to_name[belongs_to[1]]
        assert isinstance(belongs_to, str)
        return acc_id_to_name[belongs_to]
    return belongs_to


T = TypeVar("T")

args: argparse.Namespace = None


def set_args(_args):
    global args
    args = _args


def to_datetime(x, **kwargs):
    """
    Custom to_datetime with default args
    """
    if "format" not in kwargs and (
        hasattr(args, "date_format") and args.date_format is not None
    ):
        kwargs["format"] = args.date_format

    # australia normally has day first.
    if "format" not in kwargs:
        try:
            dayfirst = args.date_format_day_first
        except AttributeError:
            dayfirst = True
        kwargs["dayfirst"] = dayfirst

    return pd.to_datetime(x, **kwargs)


def group_by(
    list_of_items: Iterable[T], functor: Callable[[T], str]
) -> Dict[str, List[T]]:
    """Given a list of items and a functor that extract the item's identity, we will
    return a dictionary that are grouped by that identity."""
    grouped: Dict[str, List[T]] = {}
    for item in list_of_items:
        identity = functor(item)
        if functor(item) not in grouped:
            grouped[identity] = []
        grouped[identity].append(item)
    return grouped


def search_keywords_in_text(
    text_to_search: Optional[str], keywords: Union[str, List[str]]
) -> Union[bool, Match[str]]:
    """Return true or false depending on whether the token is found."""
    if type(keywords) is list:
        _keyword_regex = "|".join(r"\b({})\b".format(re.escape(k)) for k in keywords)
    elif type(keywords) is str:
        _keyword_regex = r"\b%s\b" % keywords
    else:
        raise ValueError(f"{keywords} is of type {type(keywords)}")

    if text_to_search is not None:
        assert type(text_to_search) is str, type(text_to_search)
        keyword_search = re.compile(_keyword_regex, re.I)
        result = keyword_search.search(text_to_search)
        if result:
            return result
    return False


def prompt_response(msg: str):
    abort = True
    try:
        try:
            user_input = input(f"{msg} [y/N/QUIT] ")
        except KeyboardInterrupt:
            print("Aborting...")
            exit(1)
        abort = user_input.strip().lower() != "y"
    except KeyboardInterrupt:
        print()
    # if abort:
    #     print("Aborting...")
    #     exit(1)
    return not abort


class FireflyIIIRulesConflictException(ValueError):
    def __init__(
        self,
        rule1: str,
        rule2: str,
        updates1: Dict,
        updates2: Dict,
    ):
        self.rule1 = rule1
        self.rule2 = rule2
        self.updates1 = updates1
        self.updates2 = updates2

        def _format_update(update: dict) -> str:
            return pprint.pformat(
                {k: v.new_val for k, v in self.updates1.items()}, indent=2, width=40
            )

        self.message = (
            f"\n"
            f"There are overlapping in between two rules:\n"
            f" - {self.rule1}\n"
            f" - {self.rule2}\n"
            f"With the updates:\n"
            f"{_format_update(self.updates1)}\n"
            f"{_format_update(self.updates2)}\n"
        )

        super().__init__(self.message)


def setup_logger(debug: bool = False):
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel("DEBUG")

    # create formatter
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)


def print_multiple_options(
    options: List[Any],
    printer_formatter: Callable[[T, int], str] = None,
) -> None:
    """
    Given multiple choices, print all options available.
    """

    def default_printer_formatter(option: Any, idx: int) -> str:
        return f" [{idx + 1:>1}] {option}"

    if printer_formatter is None:
        printer_formatter = default_printer_formatter
    for i, option in enumerate(options):
        print(printer_formatter(option=option, idx=i))


def ask_yesno(msg, default=False):
    option_str = "y/n"
    if default == True:
        option_str = "Y/n"
    elif default == False:
        option_str = "y/N"
    while True:
        user_input = input(f"{msg}: [{option_str}] ").lower().strip().lower()
        if user_input == "" and default is not None:
            return bool(default)
        if user_input == "y":
            return True
        elif user_input == "n":
            return False


def select_option(
    options: List[Any],
    query_prompt: str = "",
    print_option_functor: Callable = print_multiple_options,
    *,
    input_string_callback: Optional[Callable[[Any], bool]] = None,
    keywords: List[str] = None,
):
    """
    Select the one of the option based on given input.
    """

    def _print_query():
        if print_option_functor:
            print_option_functor(options)
        print(query_prompt)

    def _return_result(_choice):
        _options = list(options)
        selected = _options.pop(_choice)
        return selected, _options

    if keywords is not None:
        # try to auto select
        matched_options = []
        for i, opt in enumerate(options):
            if any(k.lower() in opt.lower() for k in keywords):
                matched_options.append(i)
        if len(matched_options) == 1:
            potential = matched_options[0]
            # exact match
            _print_query()
            choice = input(
                f" >> We found a potential match; is it ({potential + 1}) {options[potential]}? [Y/n] "
            )
            if choice.lower() in ("", "y"):
                return _return_result(potential)

    try:
        while True:
            _print_query()
            # for i, o in enumerate(options):
            #     print(f" [{i+1}] {o}")
            choice = input(f"Select [1-{len(options)}]: ")
            if input_string_callback:
                if input_string_callback(choice):
                    # break by user.
                    break

            print()
            try:
                choice = int(choice) - 1
            except ValueError:
                print("[ERROR] Invalid string.\n")
                continue
            if choice < 0 or choice >= len(options):
                print("[ERROR] Choice is out of range.\n")
                continue
            return _return_result(choice)
        return None, options
    except KeyboardInterrupt:
        print("\n> Aborting...")
        exit()


def my_dateutil_parser(x: str):
    if x.lower() == "now":
        return datetime.now().date()
    return dateutil_parser(x, dayfirst=True).date()


class Inequality:
    @staticmethod
    def parse(string: str):
        """
        Given string must be of the format like:
            `string1<=string2`
            `01-12-2020>25July2025`
        """
        results = re.split(r"(==|<=|>=|[=<>])", string)
        if len(results) != 3:
            raise ValueError("Given input is invalid, unable to parse 3 parts.")
        if results[1] not in ("<=", ">=", "=", "==", "<", ">"):
            raise ValueError("Given input is invalid, incorrect inequality sign.")
        return results

    @staticmethod
    def compare(
        value1: Any,
        inequality_sign: str,
        value2: Any,
    ):
        if inequality_sign == "<=":
            return value1 <= value2
        elif inequality_sign == ">=":
            return value1 >= value2
        elif inequality_sign == "<":
            return value1 < value2
        elif inequality_sign == ">":
            return value1 > value2
        elif inequality_sign in ("=", "=="):
            return value1 == value2
        raise ValueError("Inequality_sign is not valid")
