import pprint
from typing import Optional, Dict, List, Callable, TypeVar, Union, Iterable

import humanize
from firefly_iii_client.model.transaction_split_update import TransactionSplitUpdate
from firefly_iii_client.model.transaction_update import TransactionUpdate

from config_loader import config
from firefly_datatype import FireflyTransactionDataClass
from firefly_request_manager import (
    send_transaction_update,
    get_firefly_account_mappings,
)

# Globals
acc_id_to_name: Optional[Dict[str, str]] = None

TransactionUpdateValueType = Union[str, List[str]]
PendingUpdateValuesDict = Dict[str, TransactionUpdateValueType]


class PendingUpdates:
    def __init__(
        self,
        entry: FireflyTransactionDataClass,
        rule_name: str,
        updates_kwargs: PendingUpdateValuesDict,
        apply_rule: bool = False,
        merge_tags: bool = True,
    ):
        self.entry = entry
        self.updates = self.sanitise_updates(updates_kwargs)
        self.rule = rule_name

        self.apply_rule = apply_rule
        self.merge_tags = merge_tags

    def is_empty(self) -> bool:
        return len(self.updates) == 0

    def get_transaction_update(self) -> TransactionUpdate:
        if "tags" in self.updates and self.merge_tags:
            _tags = self.updates["tags"]
            self.updates["tags"] = list(set(self.entry.tags) | set(_tags))

        transaction_update = TransactionUpdate(
            apply_rules=False, transactions=[TransactionSplitUpdate(**self.updates),],
        )
        return transaction_update

    def append_updates(self, rule: str, updates: PendingUpdateValuesDict):
        updates_that_allows_duplicates = {"tags"}
        union_set = (
            set(self.updates.keys())
            & set(updates.keys()) - updates_that_allows_duplicates
        )
        # remove key from union set if the keys are the same in both updates
        for key in list(union_set):  # list to prevent size change during iteration
            if self.updates[key] == updates[key]:
                union_set -= {key}
        if len(union_set) > 0:
            raise FireflyIIIRulesConflictException(
                self.rule, rule, self.updates, updates
            )
        self.rule += f" & {rule}"
        self.updates.update(self.sanitise_updates(updates))

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
        ret += f"    date: " f"{humanize.naturaldate(self.date)}\n"
        ret += f"    desc: {self.entry.description}\n"
        for key, val in self.updates.items():
            ret += f"        > {key}:\t{self.entry[key]}\t=>\t{val}\n"
        return ret

    def sanitise_updates(self, dictionary: PendingUpdateValuesDict):
        """Skip any items that are the same as the current value"""
        # apply nice name mappings
        for key in ["source_name", "destination_name"]:
            if key in dictionary:
                if dictionary[key] in config["vendor_name_mappings"]:
                    dictionary[key] = config["vendor_name_mappings"][dictionary[key]]
        # remove duplicates values
        ret = {}
        for k, v in dictionary.items():
            if k == "tags":
                potential_tags = []
                for tag in v:
                    if tag not in self.entry[k]:
                        potential_tags.append(tag)
                if len(potential_tags) == 0:
                    continue
            else:
                if self.entry[k] == v:
                    continue
            ret[k] = v
        # apply priority settings
        for key, new_val in list(dictionary.items()):
            if self.entry[key] is None:
                continue
            existing_val = self.entry[key]
            # see if we need to apply our priority settings
            if key in config["priority"] and existing_val in config["priority"][key]:
                # the current value is part of the priority setting.
                # if the new value is not part of the priority setting, then we will use
                # the existing one (as it will default with the lowest priority)
                # if it is, we will replace the current one only if the new one has a
                # higher priority.
                if new_val in config["priority"][key]:
                    new_rank = config["priority"][key].index(new_val)
                    existing_rank = config["priority"][key].index(existing_val)
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


def get_transaction_owner(
    entry: FireflyTransactionDataClass, actual_name=False,
):
    global acc_id_to_name
    if acc_id_to_name is None:
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
    return acc_id_to_name[belongs_to]


T = TypeVar("T")


def group_by(
    list_of_items: Iterable[T], functor: Callable[[T], str]
) -> Dict[str, List[T]]:
    """Given a list of items and a functor that extract the item's identity, we will
    return a dictionary that are grouped by that identity."""
    grouped = {}
    for item in list_of_items:
        identity = functor(item)
        if functor(item) not in grouped:
            grouped[identity] = []
        grouped[identity].append(item)
    return grouped


class FireflyIIIRulesConflictException(ValueError):
    def __init__(self, rule1, rule2, updates1, updates2):
        self.rule1 = rule1
        self.rule2 = rule2
        self.updates1 = updates1
        self.updates2 = updates2

        self.message = (
            f"\n"
            f"There are overlapping in between two rules:\n"
            f" - {self.rule1}\n"
            f" - {self.rule2}\n"
            f"With the updates:\n"
            f"{pprint.pformat(self.updates1, indent=2, width=40)}\n"
            f"{pprint.pformat(self.updates2, indent=2, width=40)}\n"
        )

        super().__init__(self.message)
