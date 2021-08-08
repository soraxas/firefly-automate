import dataclasses
import pprint
from abc import abstractmethod
from typing import Dict

from config_loader import YamlItemType
from firefly_datatype import FireflyTransactionDataClass
from miscs import PendingUpdates, FireflyIIIRulesConflictException


class Rule:
    def __init__(self, pending_updates: Dict[int, PendingUpdates]):
        self.pending_updates = pending_updates

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

    @property
    def name(self):
        raise NotImplementedError()


class StopRuleProcessing(StopIteration):
    pass
