import dataclasses
import pprint
from abc import abstractmethod
from typing import Dict, Set

from config_loader import YamlItemType
from firefly_datatype import FireflyTransactionDataClass
from miscs import PendingUpdates, FireflyIIIRulesConflictException


class Rule:
    def __init__(
        self,
        base_name: str,
        pending_updates: Dict[int, PendingUpdates],
        pending_deletes: Set[int],
    ):
        self.pending_updates = pending_updates
        self.pending_deletes = pending_deletes
        self._name_base = base_name
        self.name_suffix = None

    @property
    def enable_by_default(self) -> bool:
        return True

    @property
    def base_name(self) -> str:
        self._name_base = self._sanitise_name(self._name_base)
        return self._name_base

    @property
    def name(self) -> str:
        if self.name_suffix is None:
            return self.base_name
        self.name_suffix = self._sanitise_name(self.name_suffix)
        return f"{self.base_name}__{self.name_suffix}"

    def set_all_transactions(self, transactions):
        self.transactions = transactions

    def set_rule_config(self, rule_config):
        self.rule_config = rule_config

    @staticmethod
    def _sanitise_name(name: str) -> str:
        return name.replace(" ", "-").replace("_", "-")

    def set_name_suffix(self, name_suffix):
        self.name_suffix = name_suffix

    def add_updates(
        self, entry: FireflyTransactionDataClass, new_attrs: Dict[str, YamlItemType]
    ):
        """Add a new updates to wrt to the entry"""
        # auto wrap a single tag with a list
        if "tags" in new_attrs and type(new_attrs["tags"]) is str:
            new_attrs["tags"] = [new_attrs["tags"]]

        try:
            if entry.id not in self.pending_updates:
                _updates = PendingUpdates(
                    entry,
                    rule_name=self.name,
                    updates_kwargs=new_attrs,
                )
                if not _updates.is_empty():
                    self.pending_updates[entry.id] = _updates
            else:
                self.pending_updates[entry.id].append_updates(self.name, new_attrs)
        except FireflyIIIRulesConflictException as e:
            raise ValueError(
                f"Original message:\n" f"{pprint.pformat(dataclasses.asdict(entry))}"
            ) from e

    @abstractmethod
    def process(self, entry: FireflyTransactionDataClass) -> Dict[str, YamlItemType]:
        raise NotImplementedError()


class StopRuleProcessing(StopIteration):
    pass
