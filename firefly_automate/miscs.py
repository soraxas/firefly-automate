import logging
import pprint
import re
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Match,
    Optional,
    TypeVar,
    Union,
)

from firefly_automate import firefly_request_manager

from typing import TYPE_CHECKING

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
        _keyword = "|".join(f"({re.escape(k)})" for k in keywords)
    elif type(keywords) is str:
        _keyword = keywords
    else:
        raise ValueError(f"{keywords} is of type {type(keywords)}")

    if text_to_search is not None:
        assert type(text_to_search) is str, type(text_to_search)
        keyword_search = re.compile(r"\b%s\b" % _keyword, re.I)
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
