from __future__ import annotations

"""
MongoDB audit-field helpers.

Goal:
- Ensure every insert/create/update-style write automatically sets:
  - updated_at: current UTC time
  - updated_by: current actor (user/service) identifier

Implementation:
- A lightweight proxy `AuditedDatabase` that returns `AuditedCollection` instances.
- `AuditedCollection` wraps common write methods and injects audit fields.
- A context-local "current actor" stored in a ContextVar, set by web auth middleware
  or background workers.
"""

from contextvars import ContextVar
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union, cast


_CURRENT_ACTOR: ContextVar[Optional[str]] = ContextVar("mongo_current_actor", default=None)


def set_current_actor(actor: Optional[Union[str, int]]) -> None:
    """
    Set the current actor identifier for this execution context.

    `actor` should be a stable identifier (e.g. Cognito sub, email, "scraper_worker").
    """
    if actor is None:
        _CURRENT_ACTOR.set(None)
        return
    _CURRENT_ACTOR.set(str(actor))


def get_current_actor(*, default: str = "system") -> str:
    actor = _CURRENT_ACTOR.get()
    return actor if actor else default


def _utcnow() -> datetime:
    # Codebase convention is naive UTC (datetime.utcnow()).
    return datetime.utcnow()


UpdateSpec = Union[MutableMapping[str, Any], List[MutableMapping[str, Any]]]


def _is_operator_update(update: Any) -> bool:
    return isinstance(update, Mapping) and any(str(k).startswith("$") for k in update.keys())


def _inject_audit_into_update_spec(update: Any) -> Any:
    """
    Return an update spec that guarantees updated_at/updated_by are set.

    Supports:
    - operator updates: {"$set": {...}, "$inc": {...}}
    - pipeline updates: [{"$set": ...}, ...]
    """
    now = _utcnow()
    actor = get_current_actor()
    audit = {"updated_at": now, "updated_by": actor}

    # Pipeline update: append a $set stage.
    if isinstance(update, list):
        # Copy to avoid mutating caller input.
        pipeline = [dict(stage) for stage in update]
        pipeline.append({"$set": audit})
        return pipeline

    # Operator update: merge into $set (without clobbering other operators).
    if _is_operator_update(update):
        upd = cast(MutableMapping[str, Any], dict(update))
        existing_set = upd.get("$set")
        if isinstance(existing_set, Mapping):
            merged_set = dict(existing_set)
            merged_set.update(audit)
            upd["$set"] = merged_set
        else:
            upd["$set"] = audit
        return upd

    # Replacement docs don't belong here (handled by replace_one/find_one_and_replace).
    return update


def _inject_audit_into_document(doc: Any) -> Any:
    """Mutate a document dict to include updated_at/updated_by."""
    if not isinstance(doc, MutableMapping):
        return doc
    doc["updated_at"] = _utcnow()
    doc["updated_by"] = get_current_actor()
    return doc


class AuditedCollection:
    """
    Proxy around a pymongo Collection that injects updated_at/updated_by on writes.

    This is intentionally lightweight and only overrides the write methods we use
    in this codebase (plus a few common ones for completeness). Everything else
    falls through to the underlying collection.
    """

    def __init__(self, collection):
        self._collection = collection

    # --- Inserts / creates -------------------------------------------------
    def insert_one(self, document: MutableMapping[str, Any], *args, **kwargs):
        _inject_audit_into_document(document)
        return self._collection.insert_one(document, *args, **kwargs)

    def insert_many(self, documents: Sequence[MutableMapping[str, Any]], *args, **kwargs):
        for doc in documents:
            _inject_audit_into_document(doc)
        return self._collection.insert_many(documents, *args, **kwargs)

    # --- Updates / modifies ------------------------------------------------
    def update_one(self, filter: Any, update: UpdateSpec, *args, **kwargs):
        update2 = _inject_audit_into_update_spec(update)
        return self._collection.update_one(filter, update2, *args, **kwargs)

    def update_many(self, filter: Any, update: UpdateSpec, *args, **kwargs):
        update2 = _inject_audit_into_update_spec(update)
        return self._collection.update_many(filter, update2, *args, **kwargs)

    def find_one_and_update(self, filter: Any, update: UpdateSpec, *args, **kwargs):
        update2 = _inject_audit_into_update_spec(update)
        return self._collection.find_one_and_update(filter, update2, *args, **kwargs)

    # --- Replacements ------------------------------------------------------
    def replace_one(self, filter: Any, replacement: MutableMapping[str, Any], *args, **kwargs):
        _inject_audit_into_document(replacement)
        return self._collection.replace_one(filter, replacement, *args, **kwargs)

    def find_one_and_replace(self, filter: Any, replacement: MutableMapping[str, Any], *args, **kwargs):
        _inject_audit_into_document(replacement)
        return self._collection.find_one_and_replace(filter, replacement, *args, **kwargs)

    # --- Bulk writes (best-effort) ----------------------------------------
    def bulk_write(self, requests: Sequence[Any], *args, **kwargs):
        """
        Best-effort audit injection for bulk operations.

        Supports pymongo operations:
        - InsertOne: inject into document
        - UpdateOne/UpdateMany: inject into update spec
        - ReplaceOne: inject into replacement doc
        """
        try:
            from pymongo import InsertOne, ReplaceOne, UpdateMany, UpdateOne  # type: ignore
        except Exception:  # pragma: no cover
            return self._collection.bulk_write(requests, *args, **kwargs)

        new_reqs: List[Any] = []
        for req in requests:
            if isinstance(req, InsertOne):
                doc = dict(req._doc)  # pylint: disable=protected-access
                _inject_audit_into_document(doc)
                new_reqs.append(InsertOne(doc))
            elif isinstance(req, UpdateOne):
                update2 = _inject_audit_into_update_spec(req._doc)  # pylint: disable=protected-access
                new_reqs.append(
                    UpdateOne(
                        req._filter,  # pylint: disable=protected-access
                        update2,
                        upsert=getattr(req, "_upsert", False),
                        collation=getattr(req, "_collation", None),
                        array_filters=getattr(req, "_array_filters", None),
                        hint=getattr(req, "_hint", None),
                    )
                )
            elif isinstance(req, UpdateMany):
                update2 = _inject_audit_into_update_spec(req._doc)  # pylint: disable=protected-access
                new_reqs.append(
                    UpdateMany(
                        req._filter,  # pylint: disable=protected-access
                        update2,
                        upsert=getattr(req, "_upsert", False),
                        collation=getattr(req, "_collation", None),
                        array_filters=getattr(req, "_array_filters", None),
                        hint=getattr(req, "_hint", None),
                    )
                )
            elif isinstance(req, ReplaceOne):
                replacement = dict(req._doc)  # pylint: disable=protected-access
                _inject_audit_into_document(replacement)
                new_reqs.append(
                    ReplaceOne(
                        req._filter,  # pylint: disable=protected-access
                        replacement,
                        upsert=getattr(req, "_upsert", False),
                        collation=getattr(req, "_collation", None),
                        hint=getattr(req, "_hint", None),
                    )
                )
            else:
                new_reqs.append(req)

        return self._collection.bulk_write(new_reqs, *args, **kwargs)

    # --- Pass-through ------------------------------------------------------
    def __getattr__(self, item: str):
        return getattr(self._collection, item)

    def __repr__(self) -> str:  # pragma: no cover
        return f"AuditedCollection({self._collection!r})"


class AuditedDatabase:
    """Proxy around a pymongo Database that returns audited collections."""

    def __init__(self, database):
        self._db = database

    def get_collection(self, name: str, *args, **kwargs) -> AuditedCollection:
        return AuditedCollection(self._db.get_collection(name, *args, **kwargs))

    def __getitem__(self, name: str) -> AuditedCollection:
        return AuditedCollection(self._db[name])

    def __getattr__(self, item: str):
        return getattr(self._db, item)

    def __repr__(self) -> str:  # pragma: no cover
        return f"AuditedDatabase({self._db!r})"

