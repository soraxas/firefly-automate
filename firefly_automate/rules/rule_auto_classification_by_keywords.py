from schema import Optional, Or, Schema

from firefly_automate.firefly_datatype import FireflyTransactionDataClass
from firefly_automate.miscs import search_keywords_in_text
from firefly_automate.rules.base_rule import Rule

auto_classify_schema = Schema(
    [
        Schema(
            {
                "transaction_type": str,
                "attribute_to_update": str,
                Optional("set_extracted_keyword_to_attribute", default=None): Or(
                    str, None
                ),
                "mappings": Schema({str: [str]}),
            }
        )
    ]
)


class RuleSearchKeyword(Rule):
    schema = auto_classify_schema
    enable_by_default: bool = True

    def __init__(self, *args, **kwargs):
        super().__init__("classify_transaction", *args, **kwargs)

    def process(self, entry: FireflyTransactionDataClass):
        for rule in filter(
            lambda x: x["transaction_type"] == entry.type,
            self.config,
        ):
            self.set_name_suffix(rule["attribute_to_update"])
            for tag_name_or_category, keywords in rule["mappings"].items():
                result = search_keywords_in_text(entry.description, keywords)
                if result:
                    new_attribute = {rule["attribute_to_update"]: tag_name_or_category}
                    if rule["set_extracted_keyword_to_attribute"]:
                        """
                        # because regex ignore case will destroy the capitalisation.
                        # we will manually retrieve the word that was a successful match
                        # directly from the config
                        index = [k.upper() for k in keywords].index(
                            result[0].strip().upper()
                        )
                        new_attribute[
                            rule["set_extracted_keyword_to_attribute"]
                        ] = keywords[index]
                        """
                        # because regex always priorties keyword that are earlier in the sentence,
                        # which will causes us to miss several keywords that appear later in the
                        # sentence. Instead, we will loop through all keywords and manually search all
                        desc = entry.description.upper()
                        for k in keywords:
                            if k.upper() in desc:
                                new_attribute[
                                    rule["set_extracted_keyword_to_attribute"]
                                ] = k
                                self.add_updates(entry, new_attribute)
                    else:
                        self.add_updates(entry, new_attribute)
