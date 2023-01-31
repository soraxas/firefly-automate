import dataclasses
import datetime
from dataclasses import dataclass
from typing import Optional, Dict, List


@dataclass(init=False)
class FireflyTransactionDataClass:
    id: int

    amount: str
    date: datetime.datetime
    description: str
    destination_id: str
    source_id: str
    user: str
    transaction_journal_id: int
    order: int
    currency_id: str
    currency_code: str
    currency_name: str
    currency_symbol: str
    currency_decimal_places: int
    foreign_currency_id: str
    foreign_currency_decimal_places: int
    source_name: str
    source_iban: str
    source_type: str
    destination_name: str
    destination_type: str
    budget_id: str
    category_id: str
    category_name: str
    reconciled: bool
    tags: List[str]
    internal_reference: str
    external_id: str
    original_source: str
    type: str

    _extra_attributes: Dict[str, object]

    # foreign_currency_code: Optional[str]
    # foreign_currency_symbol: Optional[str]
    # foreign_amount: Optional[str]
    # destination_iban: Optional[str]
    # budget_name: Optional[str]
    # bill_id: Optional[str]
    # bill_name: Optional[str]
    # notes: Optional[str]
    # recurrence_id: Optional[str]
    # recurrence_total: Optional[str]
    # recurrence_count: Optional[str]
    # bunq_payment_id: Optional[str]
    # import_hash_v2: str
    # sepa_cc: Optional[str]
    # sepa_ct_op: Optional[str]
    # sepa_ct_id: Optional[str]
    # sepa_db: Optional[str]
    # sepa_country: Optional[str]
    # sepa_ep: Optional[str]
    # sepa_ci: Optional[str]
    # sepa_batch_id: Optional[str]
    # interest_date: Optional[str]
    # book_date: Optional[str]
    # process_date: Optional[str]
    # due_date: Optional[str]
    # payment_date: Optional[str]
    # invoice_date: Optional[str]
    # external_url: Optional[str]

    def __init__(self, **kwargs):
        # custom init that ignore non-necessary fields, and store them in extra attr
        names = set([f.name for f in dataclasses.fields(self)])
        _extra_attrs = dict()
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)
            else:
                _extra_attrs[k] = v
        setattr(self, "_extra_attributes", _extra_attrs)

    def __getitem__(self, index):
        if type(index) != str:
            raise ValueError(str)
        return getattr(self, index)
