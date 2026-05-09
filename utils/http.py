"""Shared HTTP utilities: retry adapters, session builders, etc."""

from __future__ import annotations

import time
from typing import Any, Callable

import requests
from urllib3.util.retry import Retry


def build_retry_session(
    retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
    allowed_methods: frozenset[str] | None = None,
) -> requests.Session:
    """Build a ``requests.Session`` with automatic retry logic.

    Parameters
    ----------
    retries
        Total number of retries to allow per request.
    backoff_factor
        Back-off factor for exponential wait between retries
        (``sleep = backoff_factor * (2 ** (retry - 1))``).
    status_forcelist
        HTTP status codes that should trigger a retry.
    allowed_methods
        HTTP methods that should be retried.  Defaults to all methods.

    Returns
    -------
    requests.Session
        A session with retry-enabled transport adapter mounted for both
        ``http://`` and ``https://``.
    """
    if allowed_methods is None:
        allowed_methods = Retry.DEFAULT_ALLOWED_METHODS

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
        raise_on_status=False,
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def retry_call(
    fn: Callable[..., Any],
    *args: Any,
    retries: int = 3,
    backoff_factor: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    **kwargs: Any,
) -> Any:
    """Call *fn* with automatic retry on specified exceptions.

    Parameters
    ----------
    fn
        Callable to execute.
    retries
        Number of retry attempts after the first failure.
    backoff_factor
        Factor for exponential back-off between retries.
    exceptions
        Tuple of exception types that should trigger a retry.
    *args, **kwargs
        Passed through to *fn*.

    Returns
    -------
    Any
        The return value of *fn*.

    Raises
    ------
    Exception
        The last exception raised after all retries are exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except exceptions as exc:
            last_exc = exc
            if attempt < retries:
                sleep_time = backoff_factor * (2 ** attempt)
                time.sleep(sleep_time)
    raise last_exc  # type: ignore[misc]
