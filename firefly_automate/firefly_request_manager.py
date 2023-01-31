import datetime
import functools
import logging
from typing import Dict, Iterable

import firefly_iii_client
from firefly_iii_client import Configuration
from firefly_iii_client.api import accounts_api
from firefly_iii_client.api import transactions_api
from firefly_iii_client.model.transaction_type_filter import TransactionTypeFilter
from firefly_iii_client.model.transaction_update import TransactionUpdate
from firefly_iii_client.model.transaction_store import TransactionStore

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
