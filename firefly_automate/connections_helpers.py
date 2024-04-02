import atexit
from multiprocessing import Lock
from multiprocessing.pool import ThreadPool
from typing import Any, Callable, Dict, Iterator

import tqdm
from firefly_iii_client.schemas import BoolClass, NoneClass


def ignore_keyboard_interrupt(functor: Callable[[], Any], reason: str = "something"):
    while True:
        try:
            return functor()
        except KeyboardInterrupt:
            print(f"> Ignoring KeyboardInterrupt, because of {reason}.")


class _AsyncRequest:
    _pool = None
    _instance = None
    pool_threads: int = 8

    def __init__(self) -> None:
        if self.__class__._instance is not None:
            raise ValueError("Cannot have more than 1 instance.")
        self.__class__._instance = self

    def run(self, functor: Callable, *args, **kwargs):
        return self.pool.apply_async(functor, args=args, kwds=kwargs)

    def close(self):
        if self._pool:
            ignore_keyboard_interrupt(
                lambda: self._pool.close(), reason="waiting for jobs to finish"
            )
            ignore_keyboard_interrupt(
                lambda: self._pool.join(), reason="waiting for jobs to finish"
            )
            self._pool = None
            if hasattr(atexit, "unregister"):
                atexit.unregister(self.close)
            print("> finished all background jobs.")

    @property
    def pool(self):
        """Create thread pool on first request
        avoids instantiating unused threadpool for blocking clients.
        """
        if self._pool is None:
            atexit.register(self.close)
            self._pool = ThreadPool(self.pool_threads)
        return self._pool


AsyncRequest = _AsyncRequest()

from decimal import Decimal

import frozendict


def unwrap_none_class(data, converter: Callable):
    if data.is_none_oapg():
        return None
    return converter(data)


def DynamicSchema_to_primitives(data):
    if not hasattr(data, "_types"):
        return data

    unwrapped = SCHEMA_UNWRAP_DICT.get(frozenset(data._types))
    if unwrapped is None:
        try:
            if data.is_none_oapg():
                return None
            return str(data)
        except AttributeError:
            pass
        raise NotImplementedError(
            f"Unimplemented type {data._types}, with content: {data}"
        )
    return unwrapped(data)


"""
This is a dictionary that maps datatype (of the DynamicSchema) to a functor
that converts the given input into native python type.
"""
SCHEMA_UNWRAP_DICT = {
    frozenset({tuple}): lambda data: tuple(
        DynamicSchema_to_primitives(d) for d in data
    ),
    frozenset({NoneClass, tuple}): lambda data: unwrap_none_class(
        data, lambda _data: tuple(DynamicSchema_to_primitives(d) for d in _data)
    ),
    frozenset({frozendict.frozendict}): lambda data: {
        k: DynamicSchema_to_primitives(v) for k, v in data.items()
    },
    frozenset({str}): lambda data: str(data),
    frozenset({NoneClass, str}): lambda data: unwrap_none_class(data, str),
    frozenset({Decimal}): lambda data: str(data),
    frozenset({NoneClass, Decimal}): lambda data: unwrap_none_class(data, str),
    frozenset({BoolClass}): lambda data: bool(data),
}


class FireflyPagerWrapper:
    """This wrapper helps to retrieve all requesting data from remote host.
    Normally some api calls will return data that are scattered in different pages, and
    require additional calls to retrieve the remaining pages.
    E.g. when getting transaction from remote host, we will need to first request
    page 1, then check for response to see the total number of pages, and then
    request the rest of the pages one by one.

    This wrapper will first make a synchronised request to page one, then request the
    rest of the pages asynchronously. This functionality are wrapped as a generator
    so end-user do not need to worry about the arrival of the data, they can simply
    iterate through this list-like structure in a for loop to get all pages.
    """

    def __init__(
        self, functor: Callable, fetching_name: str = "stuff", *args, **kwargs
    ):
        self.functor = functor
        self.args = args
        self.kwargs = kwargs
        self.fetching_name = fetching_name
        self.async_responses = []
        self.first = None

    def __iter__(self):
        self.pbar = tqdm.tqdm(desc=f"fetching {self.fetching_name}")

        kwargs = dict(self.kwargs)
        # FIXME ignore return type checking for now (as the schema currently has mistakes)
        # kwargs["_check_return_type"] = False

        header_params = {
            # "X-Trace-Id": "X-Trace-Id_example",
        }

        # First we will request the first page.
        kwargs["page"] = 1
        # Then, all subsequent pages will be obtained using async
        api_response = self.functor(
            *self.args,
            query_params=kwargs,
            header_params=header_params,
        )

        api_response = DynamicSchema_to_primitives(api_response.body)

        # see how many pages we need to go through
        self.first = api_response

        # request the rest of the pages in the background.
        ##########
        self.pbar.total = int(api_response["meta"]["pagination"]["total"])
        self.pbar.update(int(api_response["meta"]["pagination"]["count"]))
        ##########

        threading_lock = Lock()

        def _update_progress(*args, **kwargs):
            ret = self.functor(*args, **kwargs)
            ret = DynamicSchema_to_primitives(ret.body)
            with threading_lock:
                self.pbar.update(int(ret["meta"]["pagination"]["count"]))
            return ret

        total_pages = int(api_response["meta"]["pagination"]["total_pages"])
        for page_num in range(2, total_pages + 1):
            kwargs["page"] = page_num
            self.async_responses.append(
                AsyncRequest.run(
                    _update_progress,
                    *self.args,
                    query_params=dict(kwargs),
                    header_params=header_params,
                    # async_req=True,
                )
            )

        return self

    def __next__(self):
        """
        The first response is synced and the rest is async (hence need to .get())
        """
        if self.first:
            ret = self.first
            self.first = None
            return ret
        if len(self.async_responses) == 0:
            raise StopIteration
        ret = self.async_responses.pop().get()
        return ret

    def data_entries(self) -> Iterator[Dict[str, Any]]:
        """This is a simple wrapper that iterate through each entry in the 'data' entry
        for all pages. This wraps nicely with the FireflyPagerWrapper
        """
        for page in self:
            for d in page["data"]:
                yield d
                # yield d.to_dict()
