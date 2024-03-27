import datetime
import functools
import logging
from typing import Dict, Iterable, Tuple

import firefly_iii_client
from firefly_iii_client import Configuration
from firefly_iii_client.api import accounts_api, transactions_api, rules_api
from firefly_iii_client.model.transaction_type_filter import TransactionTypeFilter
from firefly_iii_client.model.transaction_update import TransactionUpdate
from firefly_iii_client.model.transaction_store import TransactionStore
from firefly_iii_client.model.rule_store import RuleStore

from firefly_iii_client.model.rule_trigger_type import RuleTriggerType
from firefly_iii_client.model.rule_action_store import RuleActionStore
from firefly_iii_client.model.rule_action_keyword import RuleActionKeyword

from firefly_iii_client.model.rule_trigger_store import RuleTriggerStore
from firefly_iii_client.model.rule_trigger_keyword import RuleTriggerKeyword
from firefly_iii_client.model.rule_update import RuleUpdate
from firefly_iii_client.model.rule_action_update import RuleActionUpdate

from firefly_automate.config_loader import config, YamlItemType
from firefly_automate.connections_helpers import (
    extract_data_from_pager,
    FireflyPagerWrapper,
)
from firefly_automate.firefly_datatype import FireflyTransactionDataClass

LOGGER = logging.getLogger(__name__)


class TransactionUpdateError(Exception):
    pass


def get_firefly_client_conf() -> Configuration:
    # The client must configure the authentication and authorization parameters
    # in accordance with the API server security policy.
    # Examples for each auth method are provided below, use the example that
    # satisfies your auth use case.

    # Configure OAuth2 access token for authorization: firefly_iii_auth
    configuration = firefly_iii_client.Configuration(host=config["firefly_iii_host"])
    configuration.access_token = config["firefly_iii_token"]

    # Enter a context with an instance of the API client
    return configuration


def get_rules() -> Iterable[FireflyTransactionDataClass]:
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        # Create an instance of the API class
        api_instance = rules_api.RulesApi(api_client)
        # TransactionTypeFilter
        # Optional filter on the transaction type(s) returned. (optional)

        for rule in extract_data_from_pager(
            FireflyPagerWrapper(
                api_instance.list_rule,
                "rules",
            )
        ):
            yield rule


def fire_rules():
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        # Create an instance of the API class
        api_instance = rules_api.RulesApi(api_client)
        # TransactionTypeFilter
        # Optional filter on the transaction type(s) returned. (optional)

        for rule in extract_data_from_pager(
            FireflyPagerWrapper(
                api_instance.list_rule,
                "rules",
            )
        ):
            yield rule


def get_rule_by_title(title: str):
    for rule in get_rules():
        if rule["attributes"]["title"] == title:
            return rule
    return None


def create_rule_if_not_exists(title: str, rule_group_title: str):
    rule = get_rule_by_title(title)
    if rule is None:
        with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
            # Create an instance of the API class
            api_instance = rules_api.RulesApi(api_client)
            body = RuleStore(
                actions=[
                    RuleActionStore(
                        active=True,
                        order=0,
                        stop_processing=False,
                        type=RuleActionKeyword("convert_transfer"),
                        value="Westpac Choice",
                    )
                ],
                active=True,
                description="Auto generated rule",
                order=0,
                rule_group_title=f"AUTOGEN_{rule_group_title}",
                rule_group_id="7",
                stop_processing=False,
                strict=True,
                title=title,
                trigger=RuleTriggerType("store-journal"),
                triggers=[
                    RuleTriggerStore(
                        active=True,
                        order=0,
                        stop_processing=False,
                        type=RuleTriggerKeyword("user_action"),
                        value="tag1",
                    )
                ],
            )
            # Store a new rule
            api_response = api_instance.store_rule(
                body,
            )
    return rule


def update_rule_action(id: str, action_packs: Tuple[str, str]):
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        # Create an instance of the API class
        api_instance = rules_api.RulesApi(api_client)
        body = RuleUpdate(
            actions=[
                RuleActionUpdate(
                    active=True,
                    stop_processing=False,
                    type=RuleActionKeyword(action_type),
                    value=action_value,
                )
                for action_type, action_value in action_packs
            ],
        )
        try:
            # Update existing rule.
            api_response = api_instance.update_rule(
                id=id,
                rule_update=body,
            )
        except firefly_iii_client.ApiException as e:
            print("Exception when calling RulesApi->update_rule: %s\n" % e)
            raise e


@functools.lru_cache
def get_merge_as_transfer_rule_id():
    id = get_rule_by_title("merge-as-transfer_convert")["id"]
    if id is None:
        raise ValueError("No necessary rule found.")
    return id


# with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
#     api_instance = rules_api.RulesApi(api_client)

#     acc_id_to_name = {
#         acc["id"]: acc["attributes"]["name"]
#         for acc in extract_data_from_pager(
#             FireflyPagerWrapper(api_instance.list_account, "accounts")
#         )
#     }


@functools.lru_cache
def get_firefly_account_mappings() -> Dict[str, str]:
    """Only retrieve once, and then cache it"""
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        api_instance = accounts_api.AccountsApi(api_client)

        acc_id_to_name = {
            acc["id"]: acc["attributes"]["name"]
            for acc in extract_data_from_pager(
                FireflyPagerWrapper(api_instance.list_account, "accounts")
            )
        }
        return acc_id_to_name


def send_transaction_update(transaction_id: int, transaction_update: TransactionUpdate):
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        api_instance = transactions_api.TransactionsApi(api_client)
        try:
            api_response = api_instance.update_transaction(
                str(transaction_id), transaction_update
            )
        except firefly_iii_client.ApiException as e:
            raise TransactionUpdateError(
                f"Attempting to update transaction {transaction_id}: {transaction_update}"
            ) from e
        return api_response


def send_transaction_create(transaction_store: TransactionStore):
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        api_instance = transactions_api.TransactionsApi(api_client)
        try:
            api_response = api_instance.store_transaction(transaction_store)
        except firefly_iii_client.ApiException as e:
            raise TransactionUpdateError(
                f"Attempting to store new transaction: {transaction_store}"
            ) from e
        return api_response


def send_transaction_delete(transaction_id: int):
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        api_instance = transactions_api.TransactionsApi(api_client)
        api_response = api_instance.delete_transaction(str(transaction_id))
        return api_response


def get_transactions(
    start: datetime.date, end: datetime.date
) -> Iterable[FireflyTransactionDataClass]:
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        # Create an instance of the API class
        api_instance = transactions_api.TransactionsApi(api_client)
        # TransactionTypeFilter
        # Optional filter on the transaction type(s) returned. (optional)
        trans_type = TransactionTypeFilter("all")

        for transaction in extract_data_from_pager(
            FireflyPagerWrapper(
                api_instance.list_transaction,
                "transactions",
                start=start,
                end=end,
                type=trans_type,
            )
        ):
            transaction = transaction
            assert len(transaction["attributes"]["transactions"]) == 1

            yield FireflyTransactionDataClass(
                id=transaction["id"],
                **transaction["attributes"]["transactions"][0],
            )
