from schema import Optional, Or, Schema
from dataclasses import dataclass

from firefly_automate.data_type.transaction_type import FireflyTransactionDataClass
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
                "mappings": Schema(
                    {
                        str: [
                            Or(
                                str,
                                {str: str},
                                {str: {str: str}},
                            )
                        ]
                    }
                ),
            }
        )
    ]
)


@dataclass
class Keyword:
    keyword: str
    value: str
    priority: str

    def __init__(self, initial_value):
        self.priority = "normal"
        if type(initial_value) == str:
            self.keyword = initial_value
            self.value = initial_value
        else:
            assert type(initial_value) == dict
            assert len(initial_value) == 1
            search_term, val = list(initial_value.items())[0]
            self.keyword = search_term
            if type(val) == str:
                # A: B
                self.value = val
            else:
                assert type(val) == dict
                if "value" not in val:
                    # default as the search term
                    val["value"] = search_term
                self.value = val["value"]
                if "priority" in val:
                    self.priority = val["priority"]


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
                # create a mapping of keyword -> extracted result name

                _keyword_to_result_mapping = {}
                for k in keywords:
                    k = Keyword(k)
                    _keyword_to_result_mapping[k.keyword] = k

                result = search_keywords_in_text(
                    entry.description, list(_keyword_to_result_mapping.keys())
                )
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
                        for k, v in _keyword_to_result_mapping.items():
                            if k.upper() in desc:

                                if v.priority == "low":
                                    if getattr(
                                        entry,
                                        rule["set_extracted_keyword_to_attribute"],
                                    ) not in [
                                        "",
                                        "(unknown destination account)",
                                        None,
                                    ]:
                                        # already has value. skip.
                                        continue

                                new_attribute[
                                    rule["set_extracted_keyword_to_attribute"]
                                ] = v.value
                                self.add_updates(entry, new_attribute)

                    else:
                        self.add_updates(entry, new_attribute)
