from typing import Any, Callable, Dict, Iterator

import tqdm


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
        self.fetching_name = fetching_name
        self.kwargs = kwargs

    def __iter__(self):
        self.pbar = tqdm.tqdm(desc=f"fetching {self.fetching_name}")

        kwargs = dict(self.kwargs)
        # FIXME ignore return type checking for now (as the schema currently has mistakes)
        kwargs["_check_return_type"] = False

        # First we will request the first page.
        # Then, all subsequent pages will be obtained using async
        api_response = self.functor(page=1, *self.args, **kwargs)

        # see how many pages we need to go through
        total_pages = api_response["meta"]["pagination"]["total_pages"]
        self.first = api_response

        # request the rest of the pages in the background.
        self.async_responses = []
        for page_num in range(2, total_pages + 1):
            self.async_responses.append(
                self.functor(page=page_num, *self.args, **kwargs, async_req=True)
            )

        # ##########
        # self.pbar.total = total_pages
        # self.pbar.update()
        # ##########
        ##########
        self.pbar.total = api_response["meta"]["pagination"]["total"]
        self.pbar.update(api_response["meta"]["pagination"]["count"])
        ##########

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
            self.pbar.close()
            raise StopIteration
        ret = self.async_responses.pop().get()
        # self.pbar.update()
        self.pbar.update(ret["meta"]["pagination"]["count"])
        return ret


def extract_data_from_pager(
    pager_wrapper: FireflyPagerWrapper,
) -> Iterator[Dict[str, Any]]:
    """This is a simple wrapper that iterate through each entry in the 'data' entry
    for all pages. This wraps nicely with the FireflyPagerWrapper
    """
    for page in pager_wrapper:
        for d in page["data"]:
            yield d
            # yield d.to_dict()
