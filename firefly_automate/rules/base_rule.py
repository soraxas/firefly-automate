import dataclasses
import pprint
from abc import abstractmethod
from typing import Dict, Set

from schema import Schema

from firefly_automate.config_loader import config
from firefly_automate.data_type.pending_update import (
    PendingUpdates,
    TransactionUpdateValueType,
)
from firefly_automate.data_type.transaction_type import FireflyTransactionDataClass
from firefly_automate.miscs import FireflyIIIRulesConflictException


class Rule:
    # to be implemented by sub-classed
    schema: Schema
    enable_by_default: bool = True

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
        try:
            conf = config["rules"][base_name]
            conf = self.__class__.schema.validate(conf)
        except KeyError:
            conf = dict()
        self.config = conf

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
        self,
        entry: FireflyTransactionDataClass,
        new_attrs: Dict[str, TransactionUpdateValueType],
    ):
        """Add a new updates to wrt to the entry"""
        new_attrs = dict(new_attrs)  # later on the code update this dict.
        # auto wrap a single tag with a list
        if "tags" in new_attrs and type(new_attrs["tags"]) is str:
            new_attrs["tags"] = [new_attrs["tags"]]

        # process special_rule
        if "__CURRENT_source_destination_name" in new_attrs:
            assert entry.type in ("withdrawal", "deposit")

            new_key = (
                "source_name" if entry.type == "withdrawal" else "destination_name"
            )
            new_attrs[new_key] = new_attrs.pop("__CURRENT_source_destination_name")

        if "__OPPOSITE_source_destination_name" in new_attrs:
            assert entry.type in ("withdrawal", "deposit")

            new_key = "source_name" if entry.type == "deposit" else "destination_name"
            new_attrs[new_key] = new_attrs.pop("__OPPOSITE_source_destination_name")

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
    def process(self, entry: FireflyTransactionDataClass):
        raise NotImplementedError()


class StopRuleProcessing(StopIteration):
    pass
