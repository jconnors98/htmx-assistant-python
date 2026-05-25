from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from bson import ObjectId
from decouple import config
from flask import Flask, jsonify, render_template_string, request
from openai import OpenAI
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.mongo_audit import AuditedDatabase, set_current_actor  # noqa: E402

try:
    import boto3
except Exception:  # pragma: no cover - S3 upload is optional for this tool.
    boto3 = None


LOCAL_ACTOR = "mode_file_admin"
DEFAULT_PORT = 5077
DEFAULT_VECTOR_LIMIT = 50000
logger = logging.getLogger(__name__)


def vector_share_key_for_mode(mode_name: str) -> str:
    raw = (mode_name or "").strip().lower()
    safe = re.sub(r"[^a-z0-9_]+", "_", raw)
    safe = re.sub(r"_+", "_", safe).strip("_")
    return f"shared_with_{safe or 'default'}"


def to_jsonable(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    return value


def object_to_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return {
        key: item
        for key, item in vars(value).items()
        if not key.startswith("_")
    }


def first_value(data: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return default


class ModeFileAdmin:
    def __init__(self, *, vector_limit: int = DEFAULT_VECTOR_LIMIT, include_file_details: bool = False) -> None:
        set_current_actor(LOCAL_ACTOR)
        self.vector_limit = vector_limit
        self.include_file_details = include_file_details
        self.vector_store_id = config("OPENAI_VECTOR_STORE_ID", default=None)
        self.openai = OpenAI(api_key=config("OPENAI_API_KEY"))
        self.mongo_client = MongoClient(config("MONGO_URI"), server_api=ServerApi("1"))
        self.mongo_client.admin.command("ping")
        db_name = config("MONGO_DB", default="bcca-assistant")
        self.db = AuditedDatabase(self.mongo_client.get_database(db_name))
        self.modes = self.db.get_collection("modes")
        self.documents = self.db.get_collection("documents")
        self.scraped_content = self.db.get_collection("scraped_content")
        self.discovered_files = self.db.get_collection("discovered_files")
        self.blocked_pages = self.db.get_collection("blocked_pages")
        self.s3_bucket = config("S3_BUCKET", default=None)
        self._s3 = None
        self._last_vector_scan: List[Dict[str, Any]] = []

    def close(self) -> None:
        self.mongo_client.close()

    def s3_client(self):
        if not self.s3_bucket:
            raise RuntimeError("S3_BUCKET is not configured")
        if boto3 is None:
            raise RuntimeError("boto3 is not installed")
        if self._s3 is None:
            self._s3 = boto3.client(
                "s3",
                aws_access_key_id=config("AWS_ACCESS_KEY_ID", default=None),
                aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY", default=None),
                region_name=config("COGNITO_REGION", default=None),
            )
        return self._s3

    def list_modes(self) -> List[Dict[str, Any]]:
        projection = {
            "name": 1,
            "title": 1,
            "tags": 1,
            "user_id": 1,
            "has_files": 1,
            "has_scraped_content": 1,
            "blocked_page_urls": 1,
        }
        docs = []
        for doc in self.modes.find({}, projection).sort("name", 1):
            docs.append(to_jsonable(doc))
        return docs

    def list_mongo_documents(self) -> List[Dict[str, Any]]:
        projection = {
            "content": 0,
        }
        docs = []
        for doc in self.documents.find({}, projection).sort("mode", 1):
            doc["mongo_source"] = "documents"
            doc["filename"] = doc.get("filename") or self._filename_from_doc(doc)
            docs.append(to_jsonable(doc))
        return docs

    def list_scraped_content(self) -> List[Dict[str, Any]]:
        projection = {
            "content": 0,
            "html_content": 0,
        }
        docs = []
        cursor = self.scraped_content.find(
            {"openai_file_id": {"$exists": True, "$ne": None}},
            projection,
        ).sort("base_domain", 1)
        for doc in cursor:
            doc["mongo_source"] = "scraped_content"
            doc["filename"] = doc.get("title") or doc.get("original_url") or doc.get("normalized_url")
            docs.append(to_jsonable(doc))
        return docs

    def blocked_urls_by_mode(self, modes: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
        blocked: Dict[str, Set[str]] = {}
        for mode in modes:
            mode_name = mode.get("name")
            if not mode_name:
                continue
            blocked[mode_name] = set(mode.get("blocked_page_urls") or [])
            query_parts = []
            try:
                query_parts.append({"mode_id": ObjectId(mode["_id"])})
            except Exception:
                pass
            query_parts.append({"mode": mode_name, "user_id": mode.get("user_id")})
            try:
                for doc in self.blocked_pages.find(
                    {"$or": query_parts},
                    {"normalized_url": 1},
                ):
                    normalized_url = doc.get("normalized_url")
                    if normalized_url:
                        blocked[mode_name].add(normalized_url)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Could not load blocked pages for mode %s: %s", mode_name, exc)
        return blocked

    def filter_blocked_scraped_content(
        self,
        scraped: List[Dict[str, Any]],
        blocked_by_mode: Dict[str, Set[str]],
    ) -> List[Dict[str, Any]]:
        filtered = []
        for doc in scraped:
            normalized_url = doc.get("normalized_url")
            kept_modes = [
                mode
                for mode in (doc.get("modes") or [])
                if normalized_url not in blocked_by_mode.get(mode, set())
            ]
            if not kept_modes:
                continue
            if len(kept_modes) != len(doc.get("modes") or []):
                doc = dict(doc)
                doc["modes"] = kept_modes
            filtered.append(doc)
        return filtered

    def list_discovered_files(self) -> List[Dict[str, Any]]:
        docs = []
        for doc in self.discovered_files.find({}).sort("mode", 1):
            doc["mongo_source"] = "discovered_files"
            docs.append(to_jsonable(doc))
        return docs

    def list_vector_files(self) -> List[Dict[str, Any]]:
        if not self.vector_store_id:
            return []

        results: List[Dict[str, Any]] = []
        after = None
        while len(results) < self.vector_limit:
            page_size = min(100, self.vector_limit - len(results))
            kwargs = {"vector_store_id": self.vector_store_id, "limit": page_size}
            if after:
                kwargs["after"] = after
            page = self.openai.vector_stores.files.list(**kwargs)
            items = list(getattr(page, "data", []) or [])
            if not items:
                break
            for item in items:
                results.append(self._vector_file_summary(item))
            if not getattr(page, "has_more", False):
                break
            after = getattr(page, "last_id", None) or first_value(object_to_dict(items[-1]), ("id", "file_id"))
            if not after:
                break
        return to_jsonable(results)

    def retrieve_vector_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        if not self.vector_store_id or not file_id:
            return None
        try:
            item = self.openai.vector_stores.files.retrieve(
                vector_store_id=self.vector_store_id,
                file_id=file_id,
            )
            return to_jsonable(self._vector_file_summary(item))
        except Exception as exc:  # noqa: BLE001
            logger.info("Vector file '%s' could not be retrieved: %s", file_id, exc)
            return None

    def _vector_file_summary(self, item: Any) -> Dict[str, Any]:
        data = object_to_dict(item)
        file_id = first_value(data, ("id", "file_id"))
        attrs = first_value(data, ("attributes", "metadata"), {}) or {}
        detail = self._openai_file_detail(file_id) if self.include_file_details else {}
        return {
            "file_id": file_id,
            "vector_store_file_id": first_value(data, ("id",), file_id),
            "status": data.get("status"),
            "usage_bytes": data.get("usage_bytes"),
            "created_at": data.get("created_at"),
            "last_error": data.get("last_error"),
            "attributes": attrs,
            "filename": detail.get("filename"),
            "purpose": detail.get("purpose"),
            "bytes": detail.get("bytes"),
        }

    @staticmethod
    def add_vector_membership(
        *,
        item: Dict[str, Any],
        mode_names: List[str],
        blocked_by_mode: Dict[str, Set[str]],
        vector_ids_by_mode: Dict[str, Set[str]],
    ) -> None:
        file_id = item.get("file_id")
        attrs = item.get("attributes") or {}
        primary_mode = attrs.get("mode")
        vector_url = attrs.get("url")
        if (
            primary_mode
            and file_id
            and vector_url not in blocked_by_mode.get(primary_mode, set())
        ):
            vector_ids_by_mode.setdefault(primary_mode, set()).add(file_id)
        for mode in mode_names:
            if (
                attrs.get(vector_share_key_for_mode(mode)) == "true"
                and file_id
                and vector_url not in blocked_by_mode.get(mode, set())
            ):
                vector_ids_by_mode.setdefault(mode, set()).add(file_id)

    def build_summary(
        self,
        *,
        include_vector: bool = True,
        vector_files: Optional[List[Dict[str, Any]]] = None,
        resolve_missing_direct: bool = False,
    ) -> Dict[str, Any]:
        modes = self.list_modes()
        documents = self.list_mongo_documents()
        blocked_by_mode = self.blocked_urls_by_mode(modes)
        scraped = self.filter_blocked_scraped_content(
            self.list_scraped_content(),
            blocked_by_mode,
        )
        discovered = self.list_discovered_files()
        if include_vector:
            vector_files = list(vector_files) if vector_files is not None else self.list_vector_files()
        else:
            vector_files = []

        mode_names = [mode.get("name") for mode in modes if mode.get("name")]
        mongo_ids_by_mode: Dict[str, Set[str]] = {name: set() for name in mode_names}
        vector_ids_by_mode: Dict[str, Set[str]] = {name: set() for name in mode_names}

        for doc in documents:
            mode = doc.get("mode")
            file_id = doc.get("openai_file_id")
            if mode and file_id:
                mongo_ids_by_mode.setdefault(mode, set()).add(file_id)

        for doc in scraped:
            file_id = doc.get("openai_file_id")
            for mode in doc.get("modes") or []:
                if mode and file_id:
                    mongo_ids_by_mode.setdefault(mode, set()).add(file_id)

        vector_files_by_id = {
            item.get("file_id"): item
            for item in vector_files
            if item.get("file_id")
        }
        for item in vector_files:
            self.add_vector_membership(
                item=item,
                mode_names=mode_names,
                blocked_by_mode=blocked_by_mode,
                vector_ids_by_mode=vector_ids_by_mode,
            )

        if resolve_missing_direct:
            # Only after the scan has rendered: directly retrieve expected Mongo
            # files that were absent from the initial vector-store list scan.
            for mode_name, mongo_ids in mongo_ids_by_mode.items():
                for file_id in sorted(mongo_ids - vector_ids_by_mode.get(mode_name, set())):
                    if file_id in vector_files_by_id:
                        continue
                    item = self.retrieve_vector_file(file_id)
                    if not item:
                        continue
                    vector_files_by_id[file_id] = item
                    vector_files.append(item)
                    self.add_vector_membership(
                        item=item,
                        mode_names=mode_names,
                        blocked_by_mode=blocked_by_mode,
                        vector_ids_by_mode=vector_ids_by_mode,
                    )

        mode_summaries = []
        for mode in modes:
            name = mode.get("name")
            mongo_ids = mongo_ids_by_mode.get(name, set())
            vector_ids = vector_ids_by_mode.get(name, set())
            mode_summaries.append(
                {
                    **mode,
                    "share_key": vector_share_key_for_mode(name or ""),
                    "mongo_file_count": len(mongo_ids),
                    "vector_file_count": len(vector_ids),
                    "blocked_page_count": len(blocked_by_mode.get(name, set())),
                    "blocked_page_urls": sorted(blocked_by_mode.get(name, set())),
                    "missing_in_vector": sorted(mongo_ids - vector_ids),
                    "missing_in_mongo": sorted(vector_ids - mongo_ids),
                }
            )

        return {
            "vector_store_id": self.vector_store_id,
            "vector_limit": self.vector_limit,
            "vector_loaded": include_vector,
            "direct_missing_verified": resolve_missing_direct,
            "modes": mode_summaries,
            "documents": documents,
            "scraped_content": scraped,
            "discovered_files": discovered,
            "vector_files": vector_files,
            "generated_at": datetime.utcnow().isoformat(),
        }

    def fix_vector_sharing(self, *, mode_name: str) -> Dict[str, Any]:
        logger.info("Starting vector-sharing fix for mode '%s'", mode_name)
        mode_doc = self.modes.find_one({"name": mode_name})
        if not mode_doc:
            raise ValueError(f"Mode not found: {mode_name}")
        if not self.vector_store_id:
            raise ValueError("OPENAI_VECTOR_STORE_ID is not configured")

        share_key = vector_share_key_for_mode(mode_name)
        blocked_urls = self.blocked_urls_by_mode([to_jsonable(mode_doc)]).get(mode_name, set())
        expected_ids = self._expected_openai_file_ids_for_mode(mode_name, blocked_urls=blocked_urls)
        logger.info(
            "Mode '%s' has %s expected OpenAI file(s) after excluding %s blocked page URL(s)",
            mode_name,
            len(expected_ids),
            len(blocked_urls),
        )
        logger.info("Loading vector-store files before fixing mode '%s'", mode_name)
        vector_files = self.list_vector_files()
        vector_files_by_id = {
            item.get("file_id"): item
            for item in vector_files
            if item.get("file_id")
        }
        discoverable_ids = set()
        for file_id in sorted(expected_ids):
            item = vector_files_by_id.get(file_id)
            if not item:
                item = self.retrieve_vector_file(file_id)
                if item:
                    vector_files_by_id[file_id] = item
            attrs = (item or {}).get("attributes") or {}
            if attrs.get("mode") == mode_name or attrs.get(share_key) == "true":
                discoverable_ids.add(file_id)

        missing_ids = sorted(expected_ids - discoverable_ids)
        logger.info(
            "Mode '%s' has %s file(s) missing vector sharing tag %s",
            mode_name,
            len(missing_ids),
            share_key,
        )
        results = []
        for index, file_id in enumerate(missing_ids, start=1):
            existing_attrs = (vector_files_by_id.get(file_id) or {}).get("attributes") or {}
            logger.info(
                "[%s/%s] Fixing vector sharing for mode '%s', file '%s'",
                index,
                len(missing_ids),
                mode_name,
                file_id,
            )
            result = self._mark_vector_file_shared(
                file_id=file_id,
                share_key=share_key,
                existing_attrs=existing_attrs,
            )
            results.append(result)
            if result.get("ok"):
                logger.info(
                    "[%s/%s] Fixed file '%s' via %s",
                    index,
                    len(missing_ids),
                    file_id,
                    result.get("action"),
                )
            else:
                logger.error(
                    "[%s/%s] Failed to fix file '%s': %s",
                    index,
                    len(missing_ids),
                    file_id,
                    result.get("error"),
                )

        summary = {
            "mode": mode_name,
            "share_key": share_key,
            "expected": len(expected_ids),
            "missing_before": len(missing_ids),
            "blocked_pages_excluded": len(blocked_urls),
            "fixed": sum(1 for item in results if item.get("ok")),
            "failed": sum(1 for item in results if not item.get("ok")),
            "results": results,
        }
        logger.info(
            "Completed vector-sharing fix for mode '%s': fixed=%s failed=%s",
            mode_name,
            summary["fixed"],
            summary["failed"],
        )
        return summary

    def bulk_upload(
        self,
        *,
        mode_name: str,
        files: List[FileStorage],
        tag: str = "",
        always_include: bool = False,
        upload_s3: bool = False,
    ) -> Dict[str, Any]:
        mode_doc = self.modes.find_one({"name": mode_name})
        if not mode_doc:
            raise ValueError(f"Mode not found: {mode_name}")

        allowed_tags = mode_doc.get("tags") or []
        if tag and allowed_tags and tag not in allowed_tags:
            raise ValueError(f"Tag '{tag}' is not configured for mode '{mode_name}'")

        results = []
        for index, upload in enumerate(files, start=1):
            if not upload or not upload.filename:
                continue
            results.append(
                self._upload_one_file(
                    mode_doc=mode_doc,
                    upload=upload,
                    tag=tag,
                    always_include=always_include,
                    upload_s3=upload_s3,
                    fallback_index=index,
                )
            )

        if any(item.get("ok") for item in results):
            self.modes.update_one({"_id": mode_doc["_id"]}, {"$set": {"has_files": True}})

        return {
            "mode": mode_name,
            "tag": tag,
            "always_include": always_include,
            "upload_s3": upload_s3,
            "uploaded": sum(1 for item in results if item.get("ok")),
            "failed": sum(1 for item in results if not item.get("ok")),
            "results": results,
        }

    def _upload_one_file(
        self,
        *,
        mode_doc: Dict[str, Any],
        upload: FileStorage,
        tag: str,
        always_include: bool,
        upload_s3: bool,
        fallback_index: int,
    ) -> Dict[str, Any]:
        mode_name = mode_doc.get("name")
        raw_name = upload.filename or f"upload-{fallback_index}"
        filename = secure_filename(raw_name) or f"upload-{fallback_index}"
        data = upload.read()
        if not data:
            return {"ok": False, "filename": filename, "error": "File is empty"}

        openai_file_id = None
        vector_attached = False
        s3_key = None
        try:
            openai_file = self.openai.files.create(
                file=(filename, io.BytesIO(data)),
                purpose="assistants",
            )
            openai_file_id = openai_file.id

            if not self.vector_store_id:
                raise RuntimeError("OPENAI_VECTOR_STORE_ID is not configured")

            attrs = {
                "mode": mode_name,
                "source": "bulk_admin_local",
            }
            if tag:
                attrs["tag"] = tag
            if always_include:
                attrs["always_include"] = "true"

            self.openai.vector_stores.files.create(
                vector_store_id=self.vector_store_id,
                file_id=openai_file_id,
                attributes=attrs,
            )
            vector_attached = True

            if upload_s3:
                s3_key = self._store_in_s3(
                    mode_name=mode_name,
                    tag=tag,
                    filename=filename,
                    data=data,
                    content_type=upload.content_type,
                    always_include=always_include,
                )

            doc = {
                "user_id": mode_doc.get("user_id"),
                "mode": mode_name,
                "content": "",
                "tag": tag,
                "s3_key": s3_key,
                "openai_file_id": openai_file_id,
                "always_include": always_include,
                "source": "bulk_admin_local",
                "filename": filename,
                "uploaded_at": datetime.utcnow(),
            }
            result = self.documents.insert_one(doc)
            return {
                "ok": True,
                "filename": filename,
                "document_id": str(result.inserted_id),
                "openai_file_id": openai_file_id,
                "s3_key": s3_key,
            }
        except Exception as exc:  # noqa: BLE001
            self._cleanup_partial_upload(openai_file_id, vector_attached)
            return {
                "ok": False,
                "filename": filename,
                "openai_file_id": openai_file_id,
                "error": str(exc),
            }

    def _expected_openai_file_ids_for_mode(self, mode_name: str, *, blocked_urls: Set[str]) -> Set[str]:
        expected: Set[str] = set()
        for doc in self.documents.find(
            {"mode": mode_name, "openai_file_id": {"$exists": True, "$ne": None}},
            {"openai_file_id": 1},
        ):
            expected.add(doc["openai_file_id"])
        for doc in self.scraped_content.find(
            {"modes": mode_name, "openai_file_id": {"$exists": True, "$ne": None}},
            {"openai_file_id": 1, "normalized_url": 1},
        ):
            if doc.get("normalized_url") in blocked_urls:
                continue
            expected.add(doc["openai_file_id"])
        return expected

    def _mark_vector_file_shared(
        self,
        *,
        file_id: str,
        share_key: str,
        existing_attrs: Dict[str, Any],
    ) -> Dict[str, Any]:
        merged_attrs = dict(existing_attrs)
        merged_attrs[share_key] = "true"
        vs_files = self.openai.vector_stores.files
        updater = getattr(vs_files, "update", None) or getattr(vs_files, "modify", None)

        if callable(updater):
            try:
                updater(
                    vector_store_id=self.vector_store_id,
                    file_id=file_id,
                    attributes=merged_attrs,
                )
                return {"ok": True, "file_id": file_id, "action": "updated"}
            except Exception as update_exc:  # noqa: BLE001
                create_result = self._attach_vector_file(file_id=file_id, attributes=merged_attrs)
                if create_result.get("ok"):
                    return create_result
                create_result["update_error"] = str(update_exc)
                return create_result

        return self._attach_vector_file(file_id=file_id, attributes=merged_attrs)

    def _attach_vector_file(self, *, file_id: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        try:
            self.openai.vector_stores.files.create(
                vector_store_id=self.vector_store_id,
                file_id=file_id,
                attributes=attributes,
            )
            return {"ok": True, "file_id": file_id, "action": "attached"}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "file_id": file_id, "action": "failed", "error": str(exc)}

    def _store_in_s3(
        self,
        *,
        mode_name: str,
        tag: str,
        filename: str,
        data: bytes,
        content_type: Optional[str],
        always_include: bool,
    ) -> str:
        key = f"{mode_name}/{tag or 'untagged'}/{filename}"
        metadata = {"always-include": "true"} if always_include else {}
        self.s3_client().put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=data,
            ContentType=content_type or "application/octet-stream",
            Metadata=metadata,
        )
        return key

    def _cleanup_partial_upload(self, openai_file_id: Optional[str], vector_attached: bool) -> None:
        if not openai_file_id:
            return
        if vector_attached and self.vector_store_id:
            try:
                self.openai.vector_stores.files.delete(
                    vector_store_id=self.vector_store_id,
                    file_id=openai_file_id,
                )
            except Exception:
                pass
        try:
            self.openai.files.delete(openai_file_id)
        except Exception:
            pass

    def _openai_file_detail(self, file_id: Optional[str]) -> Dict[str, Any]:
        if not file_id:
            return {}
        try:
            return object_to_dict(self.openai.files.retrieve(file_id))
        except Exception:
            return {}

    @staticmethod
    def _filename_from_doc(doc: Dict[str, Any]) -> str:
        if doc.get("s3_key"):
            return str(doc["s3_key"]).split("/")[-1]
        if doc.get("source_url"):
            return str(doc["source_url"]).rstrip("/").split("/")[-1]
        return doc.get("openai_file_id") or str(doc.get("_id", "document"))


HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mode File Admin</title>
  <style>
    :root { color-scheme: light dark; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    body { margin: 0; background: #f5f5f7; color: #202124; }
    header { padding: 24px 32px; background: #82002d; color: #fff; }
    main { padding: 24px 32px 48px; }
    h1, h2, h3 { margin: 0 0 12px; }
    h1 { font-size: 24px; }
    h2 { font-size: 18px; }
    h3 { font-size: 15px; }
    .muted { color: #6b7280; }
    .grid { display: grid; grid-template-columns: 360px minmax(0, 1fr); gap: 20px; align-items: start; }
    .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; box-shadow: 0 1px 2px rgb(0 0 0 / 0.04); padding: 18px; }
    .mode-list { display: grid; gap: 8px; max-height: 70vh; overflow: auto; }
    .mode-btn { width: 100%; text-align: left; border: 1px solid #e5e7eb; border-radius: 10px; padding: 10px 12px; background: #fff; cursor: pointer; }
    .mode-btn.active { border-color: #82002d; box-shadow: 0 0 0 2px rgb(130 0 45 / 0.12); }
    .counts { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
    .pill { display: inline-flex; gap: 4px; align-items: center; border-radius: 999px; background: #f3f4f6; color: #374151; padding: 3px 8px; font-size: 12px; }
    .pill.warn { background: #fff7ed; color: #9a3412; }
    .pill.good { background: #ecfdf5; color: #047857; }
    form { display: grid; gap: 12px; }
    label { display: grid; gap: 5px; font-size: 13px; font-weight: 600; }
    input, select, button { font: inherit; }
    input[type="text"], select { border: 1px solid #d1d5db; border-radius: 8px; padding: 8px 10px; }
    input[type="file"] { border: 1px dashed #cbd5e1; border-radius: 8px; padding: 12px; background: #f8fafc; }
    button.primary { border: 0; border-radius: 8px; padding: 10px 14px; background: #82002d; color: #fff; cursor: pointer; }
    button.secondary { border: 1px solid #d1d5db; border-radius: 8px; padding: 8px 12px; background: #fff; cursor: pointer; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border-bottom: 1px solid #e5e7eb; padding: 8px 6px; text-align: left; vertical-align: top; }
    th { color: #4b5563; font-weight: 700; background: #fafafa; position: sticky; top: 0; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
    .section { margin-top: 18px; }
    .table-wrap { max-height: 380px; overflow: auto; border: 1px solid #e5e7eb; border-radius: 10px; }
    .toolbar { display: flex; justify-content: space-between; gap: 12px; align-items: center; margin-bottom: 12px; }
    .status { white-space: pre-wrap; border-radius: 8px; padding: 10px; background: #f8fafc; border: 1px solid #e5e7eb; font-size: 13px; }
    @media (prefers-color-scheme: dark) {
      body { background: #111827; color: #f9fafb; }
      .card, .mode-btn, button.secondary { background: #1f2937; border-color: #374151; color: #f9fafb; }
      .muted { color: #9ca3af; }
      .pill { background: #374151; color: #e5e7eb; }
      input[type="text"], select, input[type="file"], .status { background: #111827; border-color: #374151; color: #f9fafb; }
      th { background: #111827; color: #e5e7eb; }
      td, th { border-color: #374151; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Mode File Admin</h1>
    <div id="meta">Loading MongoDB and OpenAI vector-store file associations...</div>
  </header>
  <main>
    <div class="grid">
      <aside class="card">
        <div class="toolbar">
          <h2>Modes</h2>
          <button class="secondary" id="refreshBtn" type="button">Refresh</button>
        </div>
        <div id="modeList" class="mode-list"></div>
      </aside>
      <section class="card">
        <div id="details" class="muted">Select a mode to inspect attachments and upload files.</div>
      </section>
    </div>
  </main>
  <script>
    let state = null;
    let selectedMode = null;

    const $ = (id) => document.getElementById(id);
    const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;"
    }[ch]));
    const shortId = (value) => value ? `<code title="${escapeHtml(value)}">${escapeHtml(String(value).slice(0, 18))}${String(value).length > 18 ? "..." : ""}</code>` : "";

    function modeVectorFiles(mode) {
      return (state.vector_files || []).filter((item) => {
        const attrs = item.attributes || {};
        if ((mode.blocked_page_urls || []).includes(attrs.url)) {
          return false;
        }
        return attrs.mode === mode.name || attrs[mode.share_key] === "true";
      });
    }

    function modeDocuments(mode) {
      return (state.documents || []).filter((doc) => doc.mode === mode.name);
    }

    function modeScraped(mode) {
      return (state.scraped_content || []).filter((doc) => (doc.modes || []).includes(mode.name));
    }

    function renderModes() {
      const list = $("modeList");
      if (!state.modes.length) {
        list.innerHTML = `<div class="muted">No modes found.</div>`;
        return;
      }
      list.innerHTML = state.modes.map((mode) => `
        <button class="mode-btn ${selectedMode === mode.name ? "active" : ""}" data-mode="${escapeHtml(mode.name)}">
          <strong>${escapeHtml(mode.title || mode.name)}</strong>
          <div class="muted"><code>${escapeHtml(mode.name)}</code></div>
          <div class="counts">
            <span class="pill">Mongo ${mode.mongo_file_count}</span>
            <span class="pill">Vector ${state.vector_loaded ? mode.vector_file_count : "loading"}</span>
            ${mode.blocked_page_count ? `<span class="pill">Blocked ${mode.blocked_page_count}</span>` : ""}
            ${state.vector_loaded && mode.missing_in_vector.length ? `<span class="pill warn">${mode.missing_in_vector.length} missing vector</span>` : ""}
            ${state.vector_loaded && mode.missing_in_mongo.length ? `<span class="pill warn">${mode.missing_in_mongo.length} missing mongo</span>` : ""}
          </div>
        </button>
      `).join("");
      list.querySelectorAll(".mode-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          selectedMode = btn.dataset.mode;
          renderModes();
          renderDetails();
        });
      });
    }

    function renderTable(rows, columns, emptyText) {
      if (!rows.length) {
        return `<div class="status">${escapeHtml(emptyText)}</div>`;
      }
      return `
        <div class="table-wrap">
          <table>
            <thead><tr>${columns.map((col) => `<th>${escapeHtml(col.label)}</th>`).join("")}</tr></thead>
            <tbody>
              ${rows.map((row) => `
                <tr>${columns.map((col) => `<td>${col.render(row)}</td>`).join("")}</tr>
              `).join("")}
            </tbody>
          </table>
        </div>`;
    }

    function uploadForm(mode) {
      const tagOptions = [`<option value="">No tag</option>`].concat((mode.tags || []).map((tag) => (
        `<option value="${escapeHtml(tag)}">${escapeHtml(tag)}</option>`
      ))).join("");
      return `
        <form id="uploadForm">
          <h3>Bulk Add Files To <code>${escapeHtml(mode.name)}</code></h3>
          <label>Tag
            <select name="tag">${tagOptions}</select>
          </label>
          <label>Files
            <input name="files" type="file" multiple required>
          </label>
          <label><span><input name="always_include" type="checkbox" value="true"> Always include with tagged searches</span></label>
          <label><span><input name="upload_s3" type="checkbox" value="true"> Also upload to configured S3 bucket</span></label>
          <button class="primary" type="submit">Upload Selected Files</button>
          <div id="uploadStatus" class="status muted">Files are uploaded to OpenAI, attached to the vector store, and recorded in MongoDB.</div>
        </form>`;
    }

    function renderDetails() {
      const details = $("details");
      const mode = (state.modes || []).find((item) => item.name === selectedMode);
      if (!mode) {
        details.innerHTML = `<div class="muted">Select a mode to inspect attachments and upload files.</div>`;
        return;
      }

      const docs = modeDocuments(mode);
      const scraped = modeScraped(mode);
      const vector = modeVectorFiles(mode);
      details.innerHTML = `
        <div class="toolbar">
          <div>
            <h2>${escapeHtml(mode.title || mode.name)}</h2>
            <div class="muted"><code>${escapeHtml(mode.name)}</code> uses vector share key <code>${escapeHtml(mode.share_key)}</code></div>
          </div>
          <div class="counts">
            <span class="pill good">Mongo ${mode.mongo_file_count}</span>
            <span class="pill good">Vector ${state.vector_loaded ? mode.vector_file_count : "loading"}</span>
            ${mode.blocked_page_count ? `<span class="pill">Blocked ${mode.blocked_page_count}</span>` : ""}
          </div>
        </div>
        ${state.vector_loaded && (mode.missing_in_vector.length || mode.missing_in_mongo.length) ? `
          <div class="status">
Missing in vector store: ${mode.missing_in_vector.length ? mode.missing_in_vector.join(", ") : "none"}
Missing in MongoDB: ${mode.missing_in_mongo.length ? mode.missing_in_mongo.join(", ") : "none"}
${mode.missing_in_vector.length ? `
<button class="secondary" id="fixVectorBtn" type="button">Fix missing vector sharing</button>
<div id="fixVectorStatus" class="muted" style="margin-top:8px;">Adds <code>${escapeHtml(mode.share_key)}</code> to each missing vector-store file.</div>` : ""}
          </div>` : ""}
        <div class="section">${uploadForm(mode)}</div>
        <div class="section">
          <h3>MongoDB documents collection</h3>
          ${renderTable(docs, [
            { label: "Filename", render: (row) => escapeHtml(row.filename || row.s3_key || "") },
            { label: "Tag", render: (row) => escapeHtml(row.tag || "") },
            { label: "OpenAI file", render: (row) => shortId(row.openai_file_id) },
            { label: "S3 key", render: (row) => escapeHtml(row.s3_key || "") },
            { label: "Always", render: (row) => row.always_include ? "yes" : "" },
            { label: "Source", render: (row) => escapeHtml(row.source || "admin") },
          ], "No documents collection records for this mode.")}
        </div>
        <div class="section">
          <h3>MongoDB scraped_content collection</h3>
          ${renderTable(scraped, [
            { label: "Title / URL", render: (row) => escapeHtml(row.title || row.original_url || row.normalized_url || "") },
            { label: "OpenAI file", render: (row) => shortId(row.openai_file_id) },
            { label: "Domain", render: (row) => escapeHtml(row.base_domain || "") },
            { label: "Status", render: (row) => escapeHtml(row.status || "") },
            { label: "Scraped", render: (row) => escapeHtml(row.scraped_at || "") },
          ], "No scraped_content records for this mode.")}
        </div>
        <div class="section">
          <h3>OpenAI vector store</h3>
          ${!state.vector_loaded ? `<div class="status">Vector store scan is still loading. MongoDB mode data is already available.</div>` : renderTable(vector, [
            { label: "Filename", render: (row) => escapeHtml(row.filename || "") },
            { label: "OpenAI file", render: (row) => shortId(row.file_id) },
            { label: "Status", render: (row) => escapeHtml(row.status || "") },
            { label: "Mode attr", render: (row) => escapeHtml((row.attributes || {}).mode || "") },
            { label: "Tag", render: (row) => escapeHtml((row.attributes || {}).tag || "") },
            { label: "Shared", render: (row) => (row.attributes || {})[mode.share_key] === "true" ? "yes" : "" },
            { label: "Source", render: (row) => escapeHtml((row.attributes || {}).source || "") },
          ], "No vector-store files are attributed to this mode.")}
        </div>
      `;
      bindUpload(mode);
      bindFixVectorSharing(mode);
    }

    function bindFixVectorSharing(mode) {
      const button = $("fixVectorBtn");
      const status = $("fixVectorStatus");
      if (!button || !status) {
        return;
      }
      button.addEventListener("click", async () => {
        if (!confirm(`Add ${mode.share_key}=true to ${mode.missing_in_vector.length} missing file(s) for ${mode.name}?`)) {
          return;
        }
        button.disabled = true;
        status.textContent = "Fixing vector-store sharing...";
        try {
          const response = await fetch(`/api/modes/${encodeURIComponent(mode.name)}/fix-vector-sharing`, {
            method: "POST",
          });
          const payload = await response.json();
          if (!response.ok) {
            throw new Error(payload.error || "Fix failed");
          }
          status.textContent = JSON.stringify(payload, null, 2);
          await loadVectorSummary();
        } catch (err) {
          status.textContent = `Error: ${err.message}`;
        } finally {
          button.disabled = false;
        }
      });
    }

    function bindUpload(mode) {
      const form = $("uploadForm");
      const status = $("uploadStatus");
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        status.textContent = "Uploading...";
        const formData = new FormData(form);
        try {
          const response = await fetch(`/api/modes/${encodeURIComponent(mode.name)}/bulk-upload`, {
            method: "POST",
            body: formData,
          });
          const payload = await response.json();
          if (!response.ok) {
            throw new Error(payload.error || "Upload failed");
          }
          status.textContent = JSON.stringify(payload, null, 2);
          await loadSummary();
          selectedMode = mode.name;
          renderModes();
          renderDetails();
        } catch (err) {
          status.textContent = `Error: ${err.message}`;
        }
      });
    }

    async function loadSummary() {
      $("meta").textContent = "Loading modes from MongoDB...";
      const response = await fetch("/api/mongo-summary");
      state = await response.json();
      if (!response.ok) {
        $("meta").textContent = state.error || "Failed to load.";
        return;
      }
      $("meta").textContent = `Loaded ${state.modes.length} modes, ${state.documents.length} Mongo documents, ${state.scraped_content.length} scraped records. Loading OpenAI vector store...`;
      if (!selectedMode && state.modes.length) {
        selectedMode = state.modes[0].name;
      }
      renderModes();
      renderDetails();
      loadVectorSummary().catch((err) => {
        $("meta").textContent = `MongoDB loaded, but vector-store scan failed: ${err.message}`;
      });
    }

    async function loadVectorSummary() {
      const response = await fetch("/api/vector-summary");
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Failed to load vector store.");
      }
      state = payload;
      $("meta").textContent = `Vector store scan complete: ${state.vector_files.length} vector files, ${state.documents.length} Mongo documents, ${state.scraped_content.length} scraped records | ${state.generated_at}`;
      renderModes();
      renderDetails();
      if (hasMissingVectorFiles(state)) {
        await verifyMissingVectorFiles();
      }
    }

    function hasMissingVectorFiles(summary) {
      return (summary.modes || []).some((mode) => (mode.missing_in_vector || []).length > 0);
    }

    async function verifyMissingVectorFiles() {
      $("meta").textContent = `Vector store scan complete. Verifying missing files by direct lookup...`;
      const response = await fetch("/api/verify-missing-vector-files");
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Failed to verify missing vector files.");
      }
      state = payload;
      $("meta").textContent = `Vector store: ${state.vector_store_id || "not configured"} | Loaded ${state.vector_files.length} vector files after direct missing verification | ${state.generated_at}`;
      renderModes();
      renderDetails();
    }

    $("refreshBtn").addEventListener("click", loadSummary);
    loadSummary().catch((err) => {
      $("meta").textContent = err.message;
    });
  </script>
</body>
</html>
"""


def create_app(*, vector_limit: int, include_file_details: bool) -> Flask:
    app = Flask(__name__)
    admin = ModeFileAdmin(vector_limit=vector_limit, include_file_details=include_file_details)

    @app.get("/")
    def index():
        return render_template_string(HTML)

    @app.get("/api/summary")
    def summary():
        try:
            return jsonify(to_jsonable(admin.build_summary()))
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @app.get("/api/mongo-summary")
    def mongo_summary():
        try:
            return jsonify(to_jsonable(admin.build_summary(include_vector=False)))
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @app.get("/api/vector-summary")
    def vector_summary():
        try:
            vector_files = admin.list_vector_files()
            admin._last_vector_scan = vector_files
            return jsonify(to_jsonable(admin.build_summary(
                include_vector=True,
                vector_files=vector_files,
                resolve_missing_direct=False,
            )))
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @app.get("/api/verify-missing-vector-files")
    def verify_missing_vector_files():
        try:
            return jsonify(to_jsonable(admin.build_summary(
                include_vector=True,
                vector_files=admin._last_vector_scan,
                resolve_missing_direct=True,
            )))
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @app.post("/api/modes/<path:mode_name>/fix-vector-sharing")
    def fix_vector_sharing(mode_name: str):
        try:
            return jsonify(to_jsonable(admin.fix_vector_sharing(mode_name=mode_name)))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @app.post("/api/modes/<path:mode_name>/bulk-upload")
    def bulk_upload(mode_name: str):
        try:
            files = request.files.getlist("files")
            if not files:
                return jsonify({"error": "Choose at least one file"}), 400
            result = admin.bulk_upload(
                mode_name=mode_name,
                files=files,
                tag=(request.form.get("tag") or "").strip(),
                always_include=request.form.get("always_include") == "true",
                upload_s3=request.form.get("upload_s3") == "true",
            )
            return jsonify(to_jsonable(result))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    @app.teardown_appcontext
    def shutdown(_exc=None):
        # Flask may call this per request; PyMongo clients are safe to leave open
        # for the process lifetime, so cleanup is handled on interpreter exit.
        return None

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local mode/file admin page.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address. Keep localhost unless you know why.")
    parser.add_argument("--port", default=DEFAULT_PORT, type=int, help="Local port to serve the page on.")
    parser.add_argument(
        "--vector-limit",
        default=DEFAULT_VECTOR_LIMIT,
        type=int,
        help="Maximum vector-store files to inspect.",
    )
    parser.add_argument(
        "--include-file-details",
        action="store_true",
        help="Retrieve OpenAI file details for each vector file. Slower, but can show filenames for vector-only files.",
    )
    parser.add_argument("--no-browser", action="store_true", help="Do not open the page in a browser.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = create_app(vector_limit=args.vector_limit, include_file_details=args.include_file_details)
    url = f"http://{args.host}:{args.port}/"
    print("Mode File Admin is local-only by default.")
    print(f"Open {url}")
    print("Press Ctrl+C to stop.")
    if not args.no_browser:
        # Give Flask a moment to bind before the browser requests the page.
        import threading

        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
