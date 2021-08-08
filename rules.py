import re
from typing import Optional, Union, List, Callable, AnyStr, Match

from config_loader import config
from miscs import FireflyTransactionDataClass


def search_keyword_in_attribute(
    text_to_search: Optional[str], keywords: Union[str, List[str]]
) -> Union[bool, Match[AnyStr]]:
    """Return true or false depending on whether the token is found."""
    if type(keywords) is list:
        _keyword = "|".join(f"({re.escape(k)})" for k in keywords)
    elif type(keywords) is str:
        _keyword = keywords
    else:
        raise ValueError(f"{keywords} is of type {type(keywords)}")

    if text_to_search is not None:
        assert type(text_to_search) is str, type(text_to_search)
        keyword_search = re.compile(r"\b%s\b" % _keyword, re.I)
        result = keyword_search.search(text_to_search)
        if result:
            return result
    return False


def rule_search_keyword(
    entry: FireflyTransactionDataClass,
    num_of_token: Union[int, str],
    functor_to_add_updates: Callable,
) -> bool:
    for rule in filter(
        lambda x: x["num_of_token"] == num_of_token, config["rules"]["search_keyword"],
    ):
        if search_keyword_in_attribute(entry[rule["target"]], rule["keyword"]):
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

                    functor_to_add_updates(
                        entry, rule_name=rule["name"], **conditional_rule["replace"]
                    )
            if "replace" in rule:
                functor_to_add_updates(entry, rule_name=rule["name"], **rule["replace"])
            if rule["stop"]:
                return True
    return False
