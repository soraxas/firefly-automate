import dataclasses

import pandas as pd
from schema import Schema

from firefly_automate.firefly_datatype import FireflyTransactionDataClass
from firefly_automate.miscs import get_transaction_owner
from firefly_automate.rules.base_rule import Rule

remove_duplicates_schema = Schema(
    [
        Schema(
            [int],
        )
    ]
)


class RemoveDuplicates(Rule):
    schema = remove_duplicates_schema
    enable_by_default: bool = False

    def __init__(self, *args, **kwargs):
        super().__init__("remove_duplicates", *args, **kwargs)
        self.ids_that_allow_duplicates = list(map(set, self.config))
        self.df_transactions = None
        self.delete_master_id = set()
        self.ignored_entries = []
        import atexit

        def exit_handler():
            if len(self.ignored_entries) == 0:
                return
            print()
            print("===============================================")
            print("> The followings are ids that had been ignored.")
            for entries in self.ignored_entries:
                print(f"# [{entries[0][1]}] {entries[0][2]}")
                print(f"- [{', '.join(e[0] for e in entries)}]")

        atexit.register(exit_handler)

    def process(self, entry: FireflyTransactionDataClass):
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
            & (
                (self.df_transactions.source_name == entry.source_name)
                | (self.df_transactions.destination_name == entry.destination_name)
            )
            # & (self.df_transactions.type == entry.type)
            & (
                (self.df_transactions.amount.astype(float) - float(entry.amount)).abs()
                < 0.001
            )
        ]
        assert len(potential_duplicates) >= 1, "Logic error?"
        assert int(entry.id) in set(potential_duplicates.id.astype(int)), "Logic error?"
        if len(potential_duplicates) > 1:

            all_ids = set(potential_duplicates.id.astype(int))
            if any(
                len(all_ids.difference(ids)) == 0
                for ids in self.ids_that_allow_duplicates
            ):
                # these are in exceptions as specified by configs
                return

            # remove self
            potential_duplicates = potential_duplicates[
                potential_duplicates.id != entry.id
            ]
            print(f"==========================")
            print(f"  date: {entry.date}")
            print(f" amount: ${float(entry.amount)}")
            print(f"   type: {entry.type}")

            choices = [
                (entry.id, get_transaction_owner(entry, True), entry.description)
            ]
            for idx, row in potential_duplicates.iterrows():
                choices.append(
                    (row.id, get_transaction_owner(row, True), row.description)
                )

            choices = sorted(choices, key=lambda x: int(x[0]))
            show_owner = not all(choices[0][1] for c in choices)
            if not show_owner:
                print(f"    acc: {choices[0][1]}")

            for i, (id, owner, desc) in enumerate(choices):
                owner_info = f"[{owner:>10}] " if show_owner else ""
                print(f" [{i+1:>1}] {id:>4}: {owner_info}{desc}")
                # import pprint
                # pprint.pprint(row)
            while True:
                user_input = input(f">> Which one to delete? [1..{len(choices)}/N] ")
                if user_input.strip() == "":
                    self.ignored_entries.append(choices)
                    break
                try:
                    num = int(user_input)
                    if not (1 <= num <= len(choices)):
                        print(f"> Input must be between 1 and {len(choices)}")
                        continue
                    self.pending_deletes.add(choices[num - 1][0])
                    break
                except ValueError:
                    print("> Input is not numeric")

            for c in choices:
                self.delete_master_id.add(c[0])

            # for idx, row in potential_duplicates.iterrows():
