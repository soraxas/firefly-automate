from typing import Union

from schema import Optional, Or, Schema

from firefly_automate.firefly_datatype import FireflyTransactionDataClass
from firefly_automate.miscs import search_keywords_in_text
from firefly_automate.rules.base_rule import Rule, StopRuleProcessing

replace_schema = Schema({str: Or(str, [str])})

condition_schema = Schema({"field": str, "value": str})

search_keyword_schema = Schema(
    [
        Schema(
            {
                "name": str,
                Optional("num_of_token", default="ignore"): Or(str, int),
                Optional("target", default="description"): str,
                Optional("keyword", default=""): str,
                Optional("stop", default=False): bool,
                Optional("conditional"): [
                    Schema(
                        {
                            Optional("contain_keywords"): [condition_schema],
                            Optional("not_contain_keywords"): [condition_schema],
                            "replace": replace_schema,
                        }
                    )
                ],
                Optional("replace"): replace_schema,
            }
        )
    ]
)


class RuleSearchKeyword(Rule):
    schema = search_keyword_schema
    enable_by_default: bool = True

    def __init__(self, *args, **kwargs):
        super().__init__("search_keyword", *args, **kwargs)

    def process(self, entry: FireflyTransactionDataClass):
        self._process(entry, "ignore")
        self._process(entry, num_of_token=len(entry.description.split(" - ")))

    def _process(
        self, entry: FireflyTransactionDataClass, num_of_token: Union[int, str]
    ):
        for rule in filter(
            lambda x: x["num_of_token"] == num_of_token,
            self.config,
        ):
            if search_keywords_in_text(entry[rule["target"]], rule["keyword"]):
                self.set_name_suffix(rule["name"])
                if "conditional" in rule:
                    # check condition
                    for conditional_rule in rule["conditional"]:
                        if "contain_keywords" in conditional_rule:
                            # some field must contain certain values
                            if not all(
                                cond["value"] in entry[cond["field"]]
                                for cond in conditional_rule["contain_keywords"]
                            ):
                                continue
                        if "not_contain_keywords" in conditional_rule:
                            # some field must not contain certain values
                            if any(
                                cond["value"] in entry[cond["field"]]
                                for cond in conditional_rule["not_contain_keywords"]
                            ):
                                continue

                        self.add_updates(entry, conditional_rule["replace"])
                if "replace" in rule:
                    self.add_updates(entry, rule["replace"])
                if rule["stop"]:
                    raise StopRuleProcessing()
