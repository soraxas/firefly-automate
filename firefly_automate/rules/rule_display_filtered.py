import dataclasses

from firefly_automate.data_type.transaction_type import FireflyTransactionDataClass
from firefly_automate.rules.base_rule import Rule


class DisplayFiltered(Rule):
    enable_by_default: bool = False

    def __init__(self, *args, **kwargs):
        super().__init__("display_filtered", *args, **kwargs)
        self.df_transactions = None
        self.delete_master_id = set()

    def process(self, entry: FireflyTransactionDataClass):
        if entry.id in self.delete_master_id or entry.id in self.pending_deletes:
            # do not remove both the master and slave transactions
            return

        try:
            # DANGEROUS
            # The ldict = {} trick creates a substitute local namespace
            # for use inside exec
            ldict = {}  # type: ignore
            exec(f"filter = lambda x: {self.rule_config}", globals(), ldict)
            transaction_filter = ldict["filter"]
        except Exception as e:
            transaction_filter = lambda x: x
            print(e)
            raise e

        display_entry = dataclasses.asdict(entry)
        display_entry["id"] = int(display_entry["id"])
        display_entry["amount"] = float(display_entry["amount"])
        if transaction_filter(display_entry):
            # pprint.pprint({k: v for k, v in dataclasses.asdict(display_entry).items() if v is not None})
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
                print(f"{key:>16}: {display_entry[key]}")
            print()
