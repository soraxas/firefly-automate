from typing import List, Union, Dict

import yaml

YamlItemType = Union[Dict[str, object], List[object], str, int, None]


def merge_user_default_yaml(user: YamlItemType, default: YamlItemType):
    # https://stackoverflow.com/questions/823196/yaml-merge-in-python
    if isinstance(user, dict) and isinstance(default, dict):
        for k, v in default.items():
            if k not in user:
                user[k] = v
            else:
                user[k] = merge_user_default_yaml(user[k], v)
    return user


with open(r"config.yaml") as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    config = yaml.load(file, Loader=yaml.FullLoader)
    # print(config)

with open(r"config_defaults.yaml") as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    config_defaults = yaml.load(file, Loader=yaml.FullLoader)


def apply_merge(user_config: YamlItemType, default_config: YamlItemType):
    if type(user_config) is list:
        # apply each item in the list with the defaults
        for i, item in enumerate(user_config):
            merge_user_default_yaml(item, default_config[0])
    elif type(user_config) is dict:
        merge_user_default_yaml(user_config, default_config)
    else:
        raise ValueError(f"type {type(user_config)}: {user_config}")


for rule_name in config["rules"].keys():
    if rule_name in config_defaults["rules"]:
        apply_merge(config["rules"][rule_name], config_defaults["rules"][rule_name])

del config_defaults
