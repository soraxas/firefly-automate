from typing import Dict

from config_loader import config, YamlItemType
from firefly_datatype import FireflyTransactionDataClass
from miscs import search_keywords_in_text
from rules.base_rule import Rule


class RuleSearchKeyword(Rule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._name = "auto_classify"
        self._rule_config = config["rules"]["auto_classification_by_keywords"]

    @property
    def name(self):
        return self._name

    def process(self, entry: FireflyTransactionDataClass):
        for rule in filter(
            lambda x: x["transaction_type"] == entry.type,
            self._rule_config,
        ):

            for tag_name_or_category, keywords in rule["mappings"].items():
                result = search_keywords_in_text(entry.description, keywords)
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

                    self._name = (
                        f"auto_classify__{rule['transaction_type']}_"
                        f"{rule['attribute_to_update']}"
                    )
                    self.add_updates(entry, new_attribute)
