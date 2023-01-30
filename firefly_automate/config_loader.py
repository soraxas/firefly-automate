from typing import List, Union, Dict

import os
import yaml
from schema import Schema, Optional

YamlItemType = Union[Dict[str, object], List[object], str, int, None]

"""
# deprecated
def merge_user_default_yaml(user: YamlItemType, default: YamlItemType):
    # https://stackoverflow.com/questions/823196/yaml-merge-in-python
    if isinstance(user, dict) and isinstance(default, dict):
        for k, v in default.items():
            if k not in user:
                user[k] = v
            else:
                user[k] = merge_user_default_yaml(user[k], v)
    return user


# deprecated
def apply_merge(user_config: YamlItemType, default_config: YamlItemType):
    if type(user_config) is list:
        # apply each item in the list with the defaults
        for i, item in enumerate(user_config):
            merge_user_default_yaml(item, default_config[0])
    elif type(user_config) is dict:
        merge_user_default_yaml(user_config, default_config)
    else:
        raise ValueError(f"type {type(user_config)}: {user_config}")


# deprecated
with open(r"config_defaults.yaml") as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    config_defaults = yaml.safe_load(file)

...

for rule_name in config["rules"].keys():
    if rule_name in config_defaults["rules"]:
        apply_merge(config["rules"][rule_name], config_defaults["rules"][rule_name])
"""

main_config_schema = Schema(
    {
        "firefly_iii_token": str,
        "firefly_iii_host": str,
        # various rules will be validated individually within their classes
        Optional("rules"): Schema({str: object}),
        # priority should be a str maps to a list of str
        Optional("mapping_priority"): Schema({str: [str]}),
        Optional("rule_priority"): Schema({str: [str]}),
        # the mapping should be a string map to a string
        Optional("vendor_name_mappings"): Schema({str: str}),
    }
)

with open(os.path.expanduser("~/.config/firefly-automate/config.yaml")) as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    config = yaml.safe_load(file) or dict()
    # optional config setting from env var for secret keys
    for key in ["firefly_iii_host", "firefly_iii_token"]:
        val = os.getenv(key)
        if val is not None:
            config[key] = val
    config = main_config_schema.validate(config)
