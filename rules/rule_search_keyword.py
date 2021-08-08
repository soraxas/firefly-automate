from typing import Union, Dict

from config_loader import config, YamlItemType
from firefly_datatype import FireflyTransactionDataClass
from miscs import search_keywords_in_text
from rules.base_rule import Rule, StopRuleProcessing


class RuleSearchKeyword(Rule):
    @property
    def name(self):
        return "rule_search_keyword"

    def process(self, entry: FireflyTransactionDataClass) -> Dict[str, YamlItemType]:
        self._process(entry, "ignore")
        self._process(entry, num_of_token=len(entry.description.split(" - ")))

    def _process(
            self, entry: FireflyTransactionDataClass, num_of_token: Union[int, str]
    ):
        for rule in filter(
                lambda x: x["num_of_token"] == num_of_token,
                config["rules"]["search_keyword"],
        ):
            if search_keywords_in_text(entry[rule["target"]], rule["keyword"]):
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
