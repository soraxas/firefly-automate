from typing import Union, Dict
import pandas as pd
import dataclasses

from schema import Schema, Optional, Or

from config_loader import config, YamlItemType
from firefly_datatype import FireflyTransactionDataClass
from miscs import search_keywords_in_text, get_transaction_owner
from rules.base_rule import Rule, StopRuleProcessing


# replace_schema = Schema({str: Or(str, [str])})

# condition_schema = Schema({"field": str, "value": str})

# search_keyword_schema = Schema(
#     [
#         Schema(
#             {
#                 "name": str,
#                 Optional("num_of_token", default="ignore"): Or(str, int),
#                 Optional("target", default="description"): str,
#                 Optional("keyword", default=""): str,
#                 Optional("stop", default=False): bool,
#                 Optional("conditional"): [
#                     Schema(
#                         {
#                             Optional("contain_keywords"): [condition_schema],
#                             Optional("not_contain_keywords"): [condition_schema],
#                             "replace": replace_schema,
#                         }
#                     )
#                 ],
#                 Optional("replace"): replace_schema,
#             }
#         )
#     ]
# )


class RemoveDuplicates(Rule):
    def __init__(self, *args, **kwargs):
        super().__init__("remove_duplicates", *args, **kwargs)
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

        if self.df_transactions is None:
            self.df_transactions = pd.DataFrame(
                map(lambda x: dataclasses.asdict(x), self.transactions)
            )
        potential_duplicates = self.df_transactions[
            self.df_transactions.description.str.startswith(entry.description)
            & (self.df_transactions.date == entry.date)
            & (self.df_transactions.type == entry.type)
            & (
                (self.df_transactions.amount.astype(float) - float(entry.amount)).abs()
                < 0.01
            )
        ]
        assert len(potential_duplicates) >= 1, "Logic error?"
        assert int(entry.id) in set(potential_duplicates.id.astype(int)), "Logic error?"
        if len(potential_duplicates) > 1:
            # remove self
            potential_duplicates = potential_duplicates[
                potential_duplicates.id != entry.id
            ]
            print(f"==========================")
            print(f" date: {entry.date}")
            print(f" amount: ${float(entry.amount)}")
            print(f" from: {entry.description}")
            print(f"   id: {entry.id}")
            print(f"  acc: {get_transaction_owner(entry, True)}")
            for idx, row in potential_duplicates.iterrows():
                print(
                    f"     dupl: {row.description} ({row.id}: {get_transaction_owner(row, True)})"
                )
                # import pprint
                # pprint.pprint(row)
            user_input = input(">> Remove all dupl? [y/N]")
            if user_input.lower() != "y":
                return
            self.delete_master_id.add(entry.id)
            for idx, row in potential_duplicates.iterrows():
                self.pending_deletes.add(row.id)
