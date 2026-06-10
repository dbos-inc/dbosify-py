"""Activity author API, mirroring ``temporalio.activity``.

Phase 0 subset: the ``defn`` decorator. The runtime functions (``info``,
``heartbeat``, cancellation observation) land in Phases 1-3.
"""

import inspect
from typing import Any, Callable, Optional, TypeVar, Union, overload

from ._internal import registry as _registry

_F = TypeVar("_F", bound=Callable[..., Any])


@overload
def defn(fn: _F) -> _F: ...


@overload
def defn(*, name: Optional[str] = None) -> Callable[[_F], _F]: ...


def defn(
    fn: Optional[_F] = None, *, name: Optional[str] = None
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for activity functions (sync or async)."""

    def decorator(fn: _F) -> _F:
        defn = _registry.ActivityDefinition(
            name=name if name is not None else fn.__name__,
            fn=fn,
            is_async=inspect.iscoroutinefunction(fn),
        )
        setattr(fn, _registry.ACTIVITY_DEFN_ATTR, defn)
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator
