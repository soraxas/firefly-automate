from schema import Schema

from firefly_automate.firefly_datatype import FireflyTransactionDataClass
from firefly_automate.miscs import get_transaction_owner
from firefly_automate.rules.base_rule import Rule

remove_duplicates_schema = Schema({})


class DeleteNonReconciled(Rule):
    schema = remove_duplicates_schema
    enable_by_default: bool = False

    def __init__(self, *args, **kwargs):
        super().__init__("delete_non_reconciled", *args, **kwargs)

    def process(self, entry: FireflyTransactionDataClass):
        if not self.rule_config:
            print(
                "You must provide --rule-config to denote which account to reconcile!"
            )
            exit(1)

        if self.rule_config not in (entry.source_name, entry.destination_name):
            return

        if not entry.reconciled:
            print(f"==========================")
            print(f"   desc: {entry.description}")
            print(f"   From: {entry.source_name}")
            print(f"     To: {entry.destination_name}")
            print(f"   date: {entry.date}")
            print(f" amount: ${float(entry.amount)}")
            print(f"   type: {entry.type}")

            self.pending_deletes.add(entry.id)
