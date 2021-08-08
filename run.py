import dataclasses
import pprint
from typing import Dict

import tqdm
from dateutil.parser import parse as dateutil_parser

from config_loader import config
from firefly_request_manager import get_transactions
from miscs import (
    get_transaction_owner,
    FireflyTransactionDataClass,
    FireflyIIIRulesConflictException,
    PendingUpdates,
    group_by,
)
from rules import rule_search_keyword, search_keyword_in_attribute


def main():
    pending_updates: Dict[int, PendingUpdates] = {}

    def add_to_be_updated(entry: FireflyTransactionDataClass, rule_name: str, **kwargs):
        # auto wrap a single tag with a list
        if "tags" in kwargs and type(kwargs["tags"]) is str:
            kwargs["tags"] = [kwargs["tags"]]

        try:
            if entry.id not in pending_updates:
                _updates = PendingUpdates(
                    entry, rule_name=rule_name, updates_kwargs=kwargs,
                )
                if not _updates.is_empty():
                    pending_updates[entry.id] = _updates
            else:
                pending_updates[entry.id].append_updates(rule_name, kwargs)
        except FireflyIIIRulesConflictException as e:
            raise ValueError(
                f"Original message:\n" f"{pprint.pformat(dataclasses.asdict(entry))}"
            ) from e

    def process_one_transaction(entry: FireflyTransactionDataClass):

        if rule_search_keyword(entry, "ignore", add_to_be_updated):
            return

        for rule in filter(
            lambda x: x["transaction_type"] == entry.type,
            config["rules"]["auto_classification_by_keywords"],
        ):

            for tag_name_or_category, keywords in rule["mappings"].items():
                result = search_keyword_in_attribute(entry.description, keywords)
                if result:
                    new_attribute = {rule["attribute_to_update"]: tag_name_or_category}
                    if rule["set_extracted_keyword_to_attribute"]:
                        # because regex ignore case will destroy the capitalisation.
                        # we will manually retrieve the word that was a successful match
                        # directly from the config
                        index = [k.upper() for k in keywords].index(
                            result[0].strip().upper()
                        )
                        new_attribute[
                            rule["set_extracted_keyword_to_attribute"]
                        ] = keywords[index]

                    add_to_be_updated(
                        entry,
                        rule_name=f"auto_classify__{rule['transaction_type']}"
                        f"_{rule['attribute_to_update']}",
                        **new_attribute,
                    )

        desc = entry.description.split(" - ")

        if entry.type == "transfer":
            belongs_to = (entry.source_id, entry.destination_id)
            belongs_to = (entry.source_id, entry.destination_id)
            return
        elif entry.type == "opening balance":
            return

        belongs_to = get_transaction_owner(entry)

        # print(acc_id_to_name[belongs_to])

        if len(desc) == 1:
            pass
            # print(row.description)
            # print(row)

        elif len(desc) == 2:
            if rule_search_keyword(entry, 2, add_to_be_updated):
                return
            # print(belongs_to)
            # print(entry.description)

        elif len(desc) == 3:
            """These are mostly ING transactions"""

            # print(row.type)
            # print(f'from {acc_id_to_name[str(row.source_id)]}')
            # print(f'to {acc_id_to_name[str(row.destination_id)]}')

            # print(desc[0])
            # print(f'{acc_id_to_name[belongs_to]}\t{row.type}\t\t{desc[0]}')
        #
        elif len(desc) > 3:
            pass
            # print(acc_id_to_name[belongs_to])
            # print(row.description)
            # print(row.tags)

        return

    start = dateutil_parser("1 Jan 2000").date()
    end = dateutil_parser("1 Jan 2200").date()
    for data in get_transactions(start, end):
        process_one_transaction(data)

    print("========================")

    for acc, updates_in_one_acc in group_by(
        pending_updates.values(), lambda x: x.acc
    ).items():
        print(f"{acc}:")
        grouped_rule_updates = group_by(updates_in_one_acc, lambda x: x.rule)
        for rule_name, updates_in_one_rule in grouped_rule_updates.items():

            print(f" >> rule: {rule_name} <<")
            for updates in sorted(updates_in_one_rule, key=lambda x: x.date):
                print(updates)

                # updates.apply()

    if len(pending_updates) == 0:
        print("No update necessary.")
        exit()

    print("=========================")
    user_input = input(
        ">> IMPORTANT: Review the above output and see if the updates are ok: [yN]"
    )
    if user_input.lower() != "y":
        print("Aborting...")
        exit(1)

    for updates in tqdm.tqdm(pending_updates.values(), desc="Applying updates"):
        updates.apply(dry_run=False)


if __name__ == "__main__":
    main()
