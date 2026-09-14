"""Runtime state that belongs to a single credential context.

One Python process can hold several credential contexts at the same time: a
second profile or target, an embedded adapter, or sequential dbt invocations
each build their own credentials object. Token caches, token providers, Fabric
API clients and Purview clients must never cross from one of those contexts
into another, so they are stored on the credentials object they were created
for instead of in class-level or module-level singletons.

That choice has three properties worth keeping:

* the state is garbage collected together with the credentials it belongs to,
  so no process-lifetime registry of every credential (and secret) ever used
  is built up;
* serialized profile data never contains the state (it is not a dataclass
  field), and no copy of a credentials object inherits a working context:
  ``copy.copy`` carries the attribute along by reference, so ownership is
  verified on every lookup, while deep copies and unpickled credentials
  receive an empty state that the first lookup replaces;
* the state belongs to one credentials *object*, not to a set of field values.
  Objects created for an instance keep serving that instance, so mutating an
  in-use credentials object (swapping ``tenant_id`` in place, for example) is
  not a supported way to switch context — copy it, or build a new credentials
  object, and configure that before its first use. Mutations that only resolve
  configuration, such as caching the workspace ID inside the Fabric API
  client, are unaffected.
"""

import threading
from collections.abc import Callable, Hashable
from typing import Any, TypeVar

T = TypeVar("T")

_STATE_ATTR = "_fabric_runtime_state"

# Guards attaching state to a credentials object. It is only ever held for a
# dictionary lookup and the construction of an empty container: never for a
# token acquisition, an API call or any other blocking work.
_ATTACH_LOCK = threading.Lock()


class CredentialRuntimeState:
    """Container for the runtime objects of a single credential context.

    A state belongs to the one credentials object it was attached to. Copies of
    that object inherit the attribute by reference, so ``is_owned_by`` decides
    whether a lookup may use it: a copy is a credential context of its own and
    gets an empty state rather than the original's provider and clients.
    """

    def __init__(self, owner: Any = None) -> None:
        # The objects handed out below refer back to the credentials anyway, so
        # holding the owner adds no lifetime: the context is freed as one cycle.
        self._owner = owner
        # Reentrant: a factory may look up other objects of the same context,
        # for example the API client that needs this context's token provider.
        self._lock = threading.RLock()
        self._objects: dict[Hashable, Any] = {}

    def is_owned_by(self, credentials: Any) -> bool:
        """Return whether this state was attached to ``credentials`` itself."""
        return self._owner is credentials

    def get_or_create(self, key: Hashable, factory: Callable[[], T]) -> T:
        """Return the object stored under ``key``, creating it on first use.

        The factory runs while this context's lock is held, so concurrent
        callers of one context end up with the same object. The lock is
        per credential context and is never shared between contexts. When the
        factory raises, nothing is cached and a later call retries.

        Args:
            key: Identifies the object within the credential context.
            factory: Builds the object when the context does not have one yet.
        """
        with self._lock:
            if key not in self._objects:
                self._objects[key] = factory()
            created: T = self._objects[key]
            return created

    # Copying or pickling credentials must keep working even though this state
    # holds a lock. The result is an unowned, empty state; the first lookup on
    # the copy replaces it with one that belongs to the copy.
    def __copy__(self) -> "CredentialRuntimeState":
        return CredentialRuntimeState()

    def __deepcopy__(self, memo: dict) -> "CredentialRuntimeState":
        return CredentialRuntimeState()

    def __reduce__(self) -> tuple[type["CredentialRuntimeState"], tuple[()]]:
        return (CredentialRuntimeState, ())


def credential_runtime_state(credentials: Any) -> CredentialRuntimeState:
    """Return the runtime state of ``credentials``, attaching it on first use.

    State inherited from the object these credentials were copied from is
    replaced by an empty one: a copy that is configured before its first use is
    a new credential context, not the context it was copied from. Reading the
    instance dictionary directly keeps the lookup working for the credential
    doubles used in tests.

    Args:
        credentials: The credentials object identifying the context.
    """
    state = credentials.__dict__.get(_STATE_ATTR)
    if state is not None and state.is_owned_by(credentials):
        return state

    with _ATTACH_LOCK:
        state = credentials.__dict__.get(_STATE_ATTR)
        if state is None or not state.is_owned_by(credentials):
            state = CredentialRuntimeState(credentials)
            credentials.__dict__[_STATE_ATTR] = state
        return state
