from dataclasses import dataclass
from typing import Dict, List, Tuple, Union

import humanize
from firefly_iii_client.model.transaction_split_update import TransactionSplitUpdate
from firefly_iii_client.model.transaction_update import TransactionUpdate

from firefly_automate import miscs
from firefly_automate.config_loader import JsonSerializableNonNesting, config
from firefly_automate.data_type.transaction_type import FireflyTransactionDataClass
from firefly_automate.firefly_request_manager import send_transaction_update

TransactionOwnerReturnType = Union[str, Tuple[str, str]]
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
            raise miscs.FireflyIIIRulesConflictException(
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
            f"  > date: "
            f"{humanize.naturaldate(self.date)}  |  id: {self.entry.id}  |  ${float(self.entry.amount):.2f}\n"
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
        return miscs.get_transaction_owner(self.entry, True)
