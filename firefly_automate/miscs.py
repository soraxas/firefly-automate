import logging
import pprint
import re
from dataclasses import dataclass
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Match,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import humanize
from firefly_iii_client.model.transaction_split_update import TransactionSplitUpdate
from firefly_iii_client.model.transaction_update import TransactionUpdate

from firefly_automate.config_loader import JsonSerializableNonNesting, config
from firefly_automate.firefly_datatype import FireflyTransactionDataClass
from firefly_automate.firefly_request_manager import (
    get_firefly_account_mappings,
    send_transaction_update,
)

TransactionUpdateValueType = Union[str, List[str]]
PendingUpdateValuesDict = Dict[str, TransactionUpdateValueType]


@dataclass
class PendingUpdateItem:
    rule: str
    new_val: TransactionUpdateValueType

    def __eq__(self, other):
        if type(self) is type(other):
            if isinstance(self, list):
                return all(a == b for a, b in zip(self, other))
            # str
            return self.new_val == other.new_val
        raise NotImplementedError()


class PendingUpdates:
    """
    Represents all pending updates that are going to apply on this transaction.
    """

    def __init__(
        self,
        entry: FireflyTransactionDataClass,
        rule_name: str,
        updates_kwargs: PendingUpdateValuesDict,
        apply_rule: bool = True,
        merge_tags: bool = True,
    ):
        self.entry = entry
        self.updates: Dict[str, PendingUpdateItem] = self.sanitise_updates(
            rule_name, updates_kwargs
        )
        self._rules = [rule_name]

        self.apply_rule = apply_rule
        self.merge_tags = merge_tags

    @property
    def rule(self) -> str:
        return " & ".join(self._rules)

    def is_empty(self) -> bool:
        return len(self.updates) == 0

    def get_transaction_update(self) -> TransactionUpdate:
        _updates: Dict[str, JsonSerializableNonNesting] = {
            k: v.new_val for k, v in self.updates.items()
        }
        if "tags" in _updates and self.merge_tags:
            _tags = _updates["tags"]
            _updates["tags"] = list(set(self.entry.tags) | set(_tags))  # type: ignore

        # add transaction_journal_id to indicate the update on the potentially split
        # see https://github.com/firefly-iii/firefly-iii/issues/5610
        # see https://github.com/firefly-iii/firefly-iii/blob/4c27bbf06971b45f17cb939ae97e475cb416bf32/app/Validation/GroupValidation.php#L41
        assert "transaction_journal_id" not in _updates
        _updates["transaction_journal_id"] = self.entry.transaction_journal_id

        transaction_update = TransactionUpdate(
            apply_rules=self.apply_rule,
            transactions=[
                TransactionSplitUpdate(**_updates),
            ],
        )
        return transaction_update

    def append_updates(self, rule: str, updates: PendingUpdateValuesDict):
        updates_by_rule = self.sanitise_updates(rule, updates)

        updates_that_allows_duplicates = {"tags"}
        union_set = (
            set(self.updates.keys())
            & set(updates_by_rule.keys()) - updates_that_allows_duplicates
        )
        # remove key from union set if the keys are the same in both updates
        for key in list(union_set):  # list to prevent size change during iteration
            if key in config["rule_priority"]:
                # only apply this resolution if both rules are within the priority list
                if all(
                    r in config["rule_priority"][key]
                    for r in (rule, self.updates[key].rule)
                ):
                    union_set -= {key}
                    if config["rule_priority"][key].index(rule) > config[
                        "rule_priority"
                    ][key].index(self.updates[key].rule):
                        # use existing value
                        updates_by_rule.pop(key)
                    else:
                        # use new value
                        self.updates.pop(key)

        # resolve conflict if there are preset rule priority in the config file
        for key in list(union_set):
            if self.updates[key] == updates_by_rule[key]:
                union_set -= {key}
        if len(union_set) > 0:
            key = union_set.pop()  # get one of the key
            raise FireflyIIIRulesConflictException(
                self.updates[key].rule,
                rule,
                self.updates,
                updates_by_rule,
                # ' & '.join(self._rules), rule, self.updates, updates
            )
        self._rules.append(rule)
        for k, v in updates_by_rule.items():
            # self.updates[k] = PendingUpdateItem(rule, v)
            self.updates[k] = v
        # self.updates.update(updates)

    def apply(self, dry_run=True, debug=False):
        transaction_update = self.get_transaction_update()
        if debug:
            print(transaction_update)
        if not dry_run:
            api_responses = send_transaction_update(
                int(self.entry.id), transaction_update
            )
            if debug:
                print(api_responses)

    def __repr__(self):
        ret = f""
        # ret += f"      id: {self.entry.id}\n"
        # ret += f"    rule: {self.rule}\n"
        ret += (
            f"  > date: " f"{humanize.naturaldate(self.date)}  |  id: {self.entry.id}\n"
        )
        ret += f"    desc: {self.entry.description}\n"
        for k, v in self.updates.items():
            if k == "tags":
                _type = "merge" if self.merge_tags else "replace"
                ret += f"        > {k}:\t{self.entry[k]}\t=>\t{_type}{v.new_val}\n"
            else:
                ret += f"        > {k}:\t{self.entry[k]}\t=>\t{v.new_val}\n"
        return ret

    def sanitise_updates(
        self, rule_name: str, dictionary: PendingUpdateValuesDict
    ) -> Dict[str, PendingUpdateItem]:
        """Skip any items that are the same as the current value"""
        # apply nice name mappings
        for key in ["source_name", "destination_name"]:
            if key in dictionary:
                if dictionary[key] in config["vendor_name_mappings"]:
                    dictionary[key] = config["vendor_name_mappings"][dictionary[key]]
        # remove duplicates values
        ret = {}
        for k, v in dictionary.items():
            # if we are updating tag, check if the new tag already exists
            if k == "tags":
                potential_tags = []
                for tag in v:
                    if tag not in self.entry[k]:
                        potential_tags.append(tag)
                if len(potential_tags) == 0:
                    continue
            # check if the value-to-be-update is the same as the current one
            else:
                if self.entry[k] == v:
                    continue
            ret[k] = PendingUpdateItem(rule_name, v)
        # apply priority settings
        for key, new_val in list(dictionary.items()):
            if self.entry[key] is None:
                continue
            existing_val = self.entry[key]
            if new_val == existing_val:
                continue
            # see if we need to apply our priority settings
            if (
                key in config["mapping_priority"]
                and existing_val in config["mapping_priority"][key]
            ):
                # the current value is part of the priority setting.
                # if the new value is not part of the priority setting, then we will use
                # the existing one (as it will default with the lowest priority)
                # if it is, we will replace the current one only if the new one has a
                # higher priority.
                if new_val in config["mapping_priority"][key]:
                    new_rank = config["mapping_priority"][key].index(new_val)
                    existing_rank = config["mapping_priority"][key].index(existing_val)
                    if existing_rank < new_rank:
                        # remove the new update as it has lower priority
                        ret.pop(key)

        return ret

    @property
    def date(self):
        return self.entry.date

    @property
    def acc(self):
        return get_transaction_owner(self.entry, True)


TransactionOwnerReturnType = Union[str, Tuple[str, str]]


def get_transaction_owner(
    entry: FireflyTransactionDataClass,
    actual_name: bool = False,
) -> TransactionOwnerReturnType:
    belongs_to: TransactionOwnerReturnType
    acc_id_to_name = get_firefly_account_mappings()
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
