import datetime
import functools
import logging
from typing import Dict, Iterable, Tuple

import firefly_iii_client
import pandas as pd
from firefly_iii_client import Configuration
from firefly_iii_client.apis.tags import accounts_api, rules_api, transactions_api
from firefly_iii_client.model.rule_action_keyword import RuleActionKeyword
from firefly_iii_client.model.rule_action_store import RuleActionStore
from firefly_iii_client.model.rule_action_update import RuleActionUpdate
from firefly_iii_client.model.rule_store import RuleStore
from firefly_iii_client.model.rule_trigger_keyword import RuleTriggerKeyword
from firefly_iii_client.model.rule_trigger_store import RuleTriggerStore
from firefly_iii_client.model.rule_trigger_type import RuleTriggerType
from firefly_iii_client.model.rule_update import RuleUpdate
from firefly_iii_client.model.transaction_split_store import TransactionSplitStore
from firefly_iii_client.model.transaction_split_update import TransactionSplitUpdate
from firefly_iii_client.model.transaction_store import TransactionStore
from firefly_iii_client.model.transaction_type_filter import TransactionTypeFilter
from firefly_iii_client.model.transaction_update import TransactionUpdate

from firefly_automate import miscs
from firefly_automate.config_loader import config
from firefly_automate.connections_helpers import FireflyPagerWrapper
from firefly_automate.data_type.transaction_type import FireflyTransactionDataClass

LOGGER = logging.getLogger(__name__)


class TransactionUpdateError(Exception):
    pass


def get_firefly_client_conf() -> Configuration:
    # The client must configure the authentication and authorization parameters
    # in accordance with the API server security policy.
    # Examples for each auth method are provided below, use the example that
    # satisfies your auth use case.

    # Configure OAuth2 access token for authorization: firefly_iii_auth
    configuration = firefly_iii_client.Configuration(
        host=config["firefly_iii_host"].rstrip("/") + "/api"
    )
    configuration.access_token = config["firefly_iii_token"]

    # Enter a context with an instance of the API client
    return configuration


def get_rules() -> Iterable[FireflyTransactionDataClass]:
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        # Create an instance of the API class
        api_instance = rules_api.RulesApi(api_client)
        # TransactionTypeFilter
        # Optional filter on the transaction type(s) returned. (optional)

        for rule in FireflyPagerWrapper(
            api_instance.list_rule,
            "rules",
        ).data_entries():
            yield rule


def fire_rules():
    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        # Create an instance of the API class
        api_instance = rules_api.RulesApi(api_client)
        # TransactionTypeFilter
        # Optional filter on the transaction type(s) returned. (optional)

        for rule in FireflyPagerWrapper(
            api_instance.list_rule,
            "rules",
        ).data_entries():
            yield rule


def get_rule_by_title(title: str):
    for rule in get_rules():
        if rule["attributes"]["title"] == title:
            return rule
    return None


# def create_rule_if_not_exists(title: str, rule_group_title: str):
#     rule = get_rule_by_title(title)
#     if rule is None:
#         with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
#             # Create an instance of the API class
#             api_instance = rules_api.RulesApi(api_client)
#             body = RuleStore(
#                 actions=[
#                     RuleActionStore(
#                         active=True,
#                         order=0,
#                         stop_processing=False,
#                         type=RuleActionKeyword("convert_transfer"),
#                         value="Westpac Choice",
#                     )
#                 ],
#                 active=True,
#                 description="Auto generated rule",
#                 order=0,
#                 rule_group_title=f"AUTOGEN_{rule_group_title}",
#                 rule_group_id="7",
#                 stop_processing=False,
#                 strict=True,
#                 title=title,
#                 trigger=RuleTriggerType("store-journal"),
#                 triggers=[
#                     RuleTriggerStore(
#                         active=True,
#                         order=0,
#                         stop_processing=False,
#                         type=RuleTriggerKeyword("user_action"),
#                         value="tag1",
#                     )
#                 ],
#             )
#             # Store a new rule
#             api_response = api_instance.store_rule(
#                 body,
#             )
#     return rule


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
                path_params=dict(id=id),
                body=body,
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
#         for acc in FireflyPagerWrapper(api_instance.list_account, "accounts").data_entries()
#     }


def get_all_account_entries(acc_type: str = None):
    account_key = str(("accounts", acc_type))
    if account_key not in miscs.args.cache:
        with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
            api_instance = accounts_api.AccountsApi(api_client)
            kwargs = {}
            if acc_type is not None:
                kwargs["type"] = acc_type
            miscs.args.cache[account_key] = list(
                FireflyPagerWrapper(
                    api_instance.list_account, "accounts", **kwargs
                ).data_entries()
            )
    return miscs.args.cache[account_key]


@functools.lru_cache
def get_firefly_account_mappings(acc_type: str = None) -> Dict[str, str]:
    """Only retrieve once, and then cache it"""
    acc_id_to_name = {
        acc["id"]: acc["attributes"]["name"]
        for acc in get_all_account_entries(acc_type)
    }
    return acc_id_to_name


def get_firefly_account_grouped_by_type(acc_type: str = None):
    # sort by id
    return miscs.group_by(
        sorted(get_all_account_entries(acc_type), key=lambda x: int(x["id"])),
        functor=lambda x: x["attributes"]["type"],
    )


def send_transaction_update(transaction_id: int, transaction_update: TransactionUpdate):
    def _raw_send(_id, _tran_update):
        path_params = {"id": str(_id)}
        return api_instance.update_transaction(
            path_params=path_params,
            body=_tran_update,
        )

    with firefly_iii_client.ApiClient(get_firefly_client_conf()) as api_client:
        api_instance = transactions_api.TransactionsApi(api_client)
        try:
            api_response = _raw_send(transaction_id, transaction_update)
        except firefly_iii_client.ApiException as e:
            body = e.body
            if isinstance(body, bytes):
                body = body.decode()
            if "This transaction is already reconciled" in body:
                if miscs.args.always_override_reconciled or miscs.prompt_response(
                    f"> Transaction {transaction_id} is already reconciled. Override?"
                ):
                    # first remove reconcile
                    api_response = _raw_send(
                        transaction_id,
                        TransactionUpdate(
                            apply_rules=False,
                            transactions=[
                                TransactionSplitUpdate(reconciled=False),
                            ],
                        ),
                    )

                    # re-send request.
                    api_response = _raw_send(transaction_id, transaction_update)

                    # send request on setting reconciled as TRUE again
                    api_response = _raw_send(
                        transaction_id,
                        TransactionUpdate(
                            apply_rules=False,
                            transactions=[
                                TransactionSplitUpdate(reconciled=True),
                            ],
                        ),
                    )
                else:
                    return None
            else:
                raise TransactionUpdateError(
                    f"Attempting to update transaction {transaction_id}: "
                    f"{transaction_update}"
                ) from e
        return api_response


def create_transaction_store(transaction_data: Dict, apply_rules: bool = True):
    for k, v in list(transaction_data.items()):
        # replace null to none
        if pd.isnull(v):
            transaction_data[k] = None
            # transaction_data.pop(k)
        if k == "tag":
            transaction_data[k] = [v]

    return TransactionStore(
        apply_rules=apply_rules,
        transactions=[
            TransactionSplitStore(**transaction_data),
        ],
    )


def send_transaction_store(transaction_store: TransactionStore):
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
        api_response = api_instance.delete_transaction(
            path_params=dict(id=transaction_id),
        )
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

        for transaction in FireflyPagerWrapper(
            api_instance.list_transaction,
            "transactions",
            start=start,
            end=end,
            type=trans_type,
        ).data_entries():
            transaction = transaction
            assert len(transaction["attributes"]["transactions"]) == 1

            yield FireflyTransactionDataClass(
                id=transaction["id"],
                **transaction["attributes"]["transactions"][0],
            )
