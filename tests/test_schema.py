# # Import Yamale and make a schema object:
# import yamale
# schema = yamale.make_schema('./schema.yaml')
#
# # Create a Data object
# data = yamale.make_data('./config.yaml')
#
# # Validate data against the schema. Throws a ValueError if data is invalid.
# yamale.validate(schema, data)

gist = """{"description": "the description for this gist",
           "public": true,
           "files": {
               "file1.txt": {"content": "String file contents"},
               "other.txt": {"content": "Another file contents"}}}"""

gist = open("./config.yaml", "r").read()


import schema
import yaml
from schema import And, Optional, Or, Schema, Use

# gist_schema = Schema(And(Use(json.loads),  # first convert from JSON
#                           # use str since json returns unicode
#                           {Optional('description'): str,
#                            'public': bool,
#                            'files': {str: {'content': str}}}))
gist_schema = Schema(
    {
        "firefly_iii_token": str,
        "firefly_iii_host": str,
        "rules": Schema({str: object}),
        # priority should be a str maps to a list of str
        Optional("priority"): Schema({str: [str]}),
        # the mapping should be a string map to a string
        Optional("vendor_name_mappings"): Schema({str: str}),
    }
)


def parse_from_file(input_file):
    with open(input_file, "rb") as f:
        data = yaml.safe_load(f)
        gist_schema.validate(data)
        # for tag in data['tags']:
        #     gist_schema.validate(tag)
    return data


# print(parse_from_file("./config.yaml"))

search_keyword = parse_from_file("./config.yaml")["rules"]["search_keyword"]


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


result = search_keyword_schema.validate(search_keyword)
# print(result)


search_keyword_schema = Schema(
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

search_keyword = parse_from_file("./config.yaml")["rules"][
    "auto_classification_by_keywords"
]
result = search_keyword_schema.validate(search_keyword)
print(result)
