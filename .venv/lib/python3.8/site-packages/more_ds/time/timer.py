from __future__ import annotations

import time
from datetime import timedelta
from typing import Optional, Type


class Timer:
    """Simple context manager for timing blocks of code.

    With :class:`Timer` you can create a runtime context
    in which a block of code code will be timed.
    When runtime context is exited,
    the :class:`Timer` instance responsible for the runtime context
    can be queried for the :attr:`duration` or :attr:`elapsed` time.

    Examples::

        from time import sleep

        from more_ds.time import Timer
        with Timer() as t:
            # sleep for half a second
            sleep(.5)

        print(t.elapsed)  # -> 0:00:00.501864
    """

    start: int
    """Start of time measurement expressed as time in nanoseconds."""

    duration: int
    """Duration of the time measurement expressed as elapsed nanoseconde."""

    def __init__(self) -> None:  # noqa: D107 nothing to document.
        self.start = 0
        self.duration = 0

    def __enter__(self) -> Timer:  # noqa: D105 well known magic method, not documenting it.
        self.start = time.perf_counter_ns()
        return self

    def __exit__(  # noqa: D105 well knows magic method, not documenting it.
        self,
        _exc_type: Optional[Type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[BaseException],
    ) -> None:
        self.duration = time.perf_counter_ns() - self.start

    @property
    def elapsed(self) -> timedelta:
        """Return the time elapsed in :class:`Timer`'s context.

        Returns:
            Time elapsed in :class:`Timer`'s context as a :class:`~datetime.timedelta` object.
        """
        return timedelta(microseconds=self.duration / 1_000)
