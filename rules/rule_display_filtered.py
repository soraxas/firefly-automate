from typing import Union, Dict
import pandas as pd
import dataclasses
import pprint
import copy

from schema import Schema, Optional, Or

from config_loader import config, YamlItemType
from firefly_datatype import FireflyTransactionDataClass
from miscs import search_keywords_in_text, get_transaction_owner
from rules.base_rule import Rule, StopRuleProcessing


class DisplayFiltered(Rule):
    def __init__(self, *args, **kwargs):
        super().__init__("display_filtered", *args, **kwargs)
        # self._rule_config = config["rules"]["search_keyword"]
        # self._rule_config = search_keyword_schema.validate(self._rule_config)
        self.df_transactions = None
        self.delete_master_id = set()

    @property
    def enable_by_default(self) -> bool:
        return False

    def process(self, entry: FireflyTransactionDataClass) -> Dict[str, YamlItemType]:
        if entry.id in self.delete_master_id or entry.id in self.pending_deletes:
            # do not remove both the master and slave transactions
            return

        try:
            # DANGEROUS
            ldict = {}
            exec(f"filter = lambda x: {self.rule_config}", globals(), ldict)
            transaction_filter = ldict["filter"]
        except Exception as e:
            transaction_filter = lambda x: x
            print(e)
            raise e

        entry = copy.deepcopy(entry)
        entry.id = int(entry.id)
        entry.amount = float(entry.amount)
        if transaction_filter(entry):
            # pprint.pprint({k: v for k, v in dataclasses.asdict(entry).items() if v is not None})
            for key in [
                "id",
                "type",
                "date",
                "amount",
                "description",
                "category_name",
                "source_name",
                "destination_name",
                "tags",
            ]:
                print(f"{key:>16}: {entry[key]}")
            print()
