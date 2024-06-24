from typing import Union, Dict
from abc import ABC, abstractmethod

from schema import Optional, Or, Schema

from firefly_automate.data_type.transaction_type import FireflyTransactionDataClass
from firefly_automate.miscs import search_keywords_in_text
from firefly_automate.rules.base_rule import Rule, StopRuleProcessing

replace_schema = Schema({str: Or(str, [str])})


class Conditional(ABC):

    @abstractmethod
    def parse(self):
        pass


class TransactionTypeCond(Conditional):

    def parse(self):
        pass


def cond_transaction_type(entry: FireflyTransactionDataClass, conditional_rule):
    return entry["type"] == conditional_rule


def cond_and(entry: FireflyTransactionDataClass, conditional_rule):
    return all(unit_conditional_parser(entry, rule) for rule in conditional_rule)


def cond_or(entry: FireflyTransactionDataClass, conditional_rule):
    return any(unit_conditional_parser(entry, rule) for rule in conditional_rule)


def cond_contain_keywords(
    entry: FireflyTransactionDataClass, conditional_rule, exact_match: bool
):
    if exact_match:
        matcher = lambda x, y: x == y
    else:
        matcher = lambda x, y: x in y

    for _key, _val in conditional_rule.items():
        cur_val = entry[_key]
        if cur_val is None or not matcher(_val, cur_val):
            return False
    return True


def cond_amount_range(entry: FireflyTransactionDataClass, conditional_rule):
    return (
        conditional_rule.get("min", float("-inf"))
        <= float(entry["amount"])
        <= conditional_rule.get("max", float("inf"))
    )


def unit_conditional_parser(entry: FireflyTransactionDataClass, conditional_rule: Dict):
    assert len(conditional_rule) == 1
    key, val = list(conditional_rule.items())[0]
    if key == "transaction_type":
        return cond_transaction_type(entry, val)
    # elif key == "negate":
    #     return cond_transaction_type(entry, val)
    elif key == "contain_keywords":
        return cond_contain_keywords(entry, val, exact_match=False)
    elif key == "match_exactly":
        return cond_contain_keywords(entry, val, exact_match=True)
    elif key == "and":
        return cond_and(entry, val)
    elif key == "or":
        return cond_or(entry, val)
    elif key == "amount_range":
        return cond_amount_range(entry, val)
    raise NotImplementedError(f"unknown cond {key} with val {val}")


_and_children = []
_or_children = []
UnitConditionalSchema = Or(
    Schema({Optional("and"): _and_children}),
    Schema({Optional("or"): _or_children}),
    Schema({Optional("transaction_type"): str}),
    Schema({Optional("contain_keywords"): Schema({str: str})}),
    Schema({Optional("match_exactly"): Schema({str: str})}),
    # Schema({Optional("negate"): UnitConditionalSchema}),
    Schema(
        {
            Optional("amount_range"): {
                Optional("min"): Or(int, float),
                Optional("max"): Or(int, float),
            }
        }
    ),
)
_and_children.append(UnitConditionalSchema)
_or_children.append(UnitConditionalSchema)


search_keyword_schema = Schema(
    [
        Schema(
            {
                Optional("name"): str,
                # Optional("num_of_token", default="ignore"): Or(str, int),
                # Optional("target", default="description"): str,
                # Optional("keyword", default=""): str,
                Optional("stop", default=False): bool,
                "conditional": UnitConditionalSchema,
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
        # self._process(entry, num_of_token=len(entry.description.split(" - ")))

    def _process(
        self, entry: FireflyTransactionDataClass, num_of_token: Union[int, str]
    ):
        for rule in self.config:
            # for rule in filter(
            #     lambda x: x["num_of_token"] == num_of_token,
            #     self.config,
            # ):
            if "name" not in rule:
                rule["name"] = f"unnamed__[{rule['conditional']}]"
            self.set_name_suffix(rule["name"])
            if "conditional" in rule and not unit_conditional_parser(
                entry, rule["conditional"]
            ):
                continue
            if "replace" in rule:
                self.add_updates(entry, rule["replace"])
            if rule["stop"]:
                raise StopRuleProcessing()
