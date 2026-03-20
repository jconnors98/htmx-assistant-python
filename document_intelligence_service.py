from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional
from uuid import uuid4

from bson import ObjectId
from document_intelligence_storage import DocumentIntelligenceStorage
from models.metadata import BidPackage, DocumentMetadata, ProjectContext, Section
from tools import DocumentToolbox
from tools.extract import ZipExtractionError

import logging

logger = logging.getLogger(__name__)


class DocumentIntelligenceService:
    """
    Coordinates doc-intel uploads, search, packaging, and worker-side execution.
    """

    def __init__(
        self,
        *,
        modes_collection,
        projects_collection,
        documents_collection,
        jobs_collection,
        storage_dir: str,
        toolbox: DocumentToolbox,
        storage: DocumentIntelligenceStorage,
        doc_client=None,
        expiry_minutes: int = 30,
        max_pdf_seconds: int = 90,
        max_zip_depth: int = 3,
        max_zip_members: int = 500,
        max_zip_member_size_bytes: int = 200 * 1024 * 1024,
        max_zip_total_size_bytes: int = 2 * 1024 * 1024 * 1024,
        max_zip_compression_ratio: float = 150.0,
        max_text_file_bytes: int = 20 * 1024 * 1024,
        max_text_chars: int = 180_000,
        max_excerpt_chars: int = 24_000,
        max_embed_chars: int = 24_000,
        max_image_pixels: int = 35_000_000,
    ):
        self.modes = modes_collection
        self.projects = projects_collection
        self.documents = documents_collection
        self.jobs = jobs_collection
        self.toolbox = toolbox
        self.storage = storage
        self.doc_client = doc_client
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.expiry_minutes = expiry_minutes
        self.max_pdf_seconds = max_pdf_seconds
        self.max_zip_depth = max_zip_depth
        self.max_zip_members = max_zip_members
        self.max_zip_member_size_bytes = max_zip_member_size_bytes
        self.max_zip_total_size_bytes = max_zip_total_size_bytes
        self.max_zip_compression_ratio = max_zip_compression_ratio
        self.max_text_file_bytes = max_text_file_bytes
        self.max_text_chars = max_text_chars
        self.max_excerpt_chars = max_excerpt_chars
        self.max_embed_chars = max_embed_chars
        self.max_image_pixels = max_image_pixels

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def cleanup_expired_documents(self) -> int:
        expiry_threshold = datetime.utcnow() - timedelta(minutes=self.expiry_minutes)
        projects = self.projects.find({"updated_at": {"$lt": expiry_threshold.isoformat()}})

        cleaned_count = 0
        for doc in projects:
            project = ProjectContext.from_dict(doc)
            self._delete_project_files(project)
            self.documents.delete_many({"session_id": project.session_id})
            self.jobs.delete_many({"session_id": project.session_id})
            self.projects.delete_one({"session_id": project.session_id})
            cleaned_count += 1

        if cleaned_count > 0:
            logger.info(f"DocIntel: cleaned up {cleaned_count} expired projects.")

        return cleaned_count

    def _delete_project_files(self, project: ProjectContext) -> None:
        for document in self._get_project_documents(project.session_id or ""):
            self.storage.delete(document.to_dict())
            try:
                if document.file_path and os.path.exists(document.file_path):
                    parent_dir = Path(document.file_path).parent
                    if parent_dir.exists() and parent_dir.is_dir() and str(self.storage_dir) in str(parent_dir):
                        shutil.rmtree(parent_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Error deleting file {document.file_path}: {e}")

        for package in project.packages:
            self.storage.delete(package.to_dict())
            for path in [package.output_pdf_path, package.output_zip_path]:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception as e:
                        logger.error(f"Error deleting package file {path}: {e}")

    def ingest_files(self, mode_doc: Dict[str, Any], session_id: str, files: Iterable[Any]) -> Dict[str, Any]:
        if not session_id:
            raise ValueError("session_id is required for document intelligence.")
        if not self._is_feature_enabled(mode_doc):
            raise ValueError("Document intelligence is not enabled for this mode.")
        project = self._ensure_project(mode_doc, session_id)
        uploaded_files: List[Dict[str, Any]] = []
        for file_storage in files:
            if not getattr(file_storage, "filename", None):
                continue
            uploaded_files.append(
                self.storage.save_upload(
                    file_storage,
                    session_id=session_id,
                    prefix="uploads",
                )
            )

        if not uploaded_files:
            return {"accepted": False, "error": "No files were supplied."}

        project.status = "queued"
        project.last_error = None
        project.last_ingest_job_id = None
        project.touch()
        self._save_project(project)

        if not self.doc_client:
            result = self.process_ingest_job(
                {
                    "session_id": session_id,
                    "mode_id": project.mode_id,
                    "mode_name": project.mode_name,
                    "uploaded_files": uploaded_files,
                }
            )
            return {"accepted": True, **result}

        job_id = self.doc_client.queue_ingest(
            session_id=session_id,
            mode_id=str(project.mode_id or mode_doc["_id"]),
            mode_name=mode_doc.get("name", ""),
            uploaded_files=uploaded_files,
        )
        project.last_ingest_job_id = job_id
        project.status = "queued"
        project.touch()
        self._save_project(project)
        return {
            "accepted": True,
            "job_id": job_id,
            "status": "queued",
            "uploaded_files": [
                {
                    "original_filename": item.get("original_filename"),
                    "storage_key": item.get("storage_key"),
                }
                for item in uploaded_files
            ],
        }

    def search(self, session_id: str, query: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        project = self._get_project_by_session(session_id)
        if not project:
            return []
        documents = [doc.to_dict() for doc in self._get_project_documents(session_id)]
        return self.toolbox.search_documents(query, documents, filters=filters)

    def build_package(self, mode_doc: Dict[str, Any], session_id: str, plan_dict: Dict[str, Any], output: str = "pdf") -> Dict[str, Any]:
        project = self._get_project_by_session(session_id)
        if not project:
            raise ValueError("No project context found for mode.")

        package = BidPackage(
            title=plan_dict.get("package_title", "Bid Package"),
            sections=[Section.from_dict(section) for section in plan_dict.get("sections", [])],
            file_type=output,
            status="queued",
        )
        project.packages.append(package)
        project.status = "queued"
        project.touch()
        self._save_project(project)

        if not self.doc_client:
            result = self.process_package_job(
                {
                    "session_id": session_id,
                    "mode_id": str(project.mode_id or mode_doc["_id"]),
                    "mode_name": mode_doc.get("name", ""),
                    "package_id": package.package_id,
                    "output": output,
                    "plan": plan_dict,
                }
            )
            return result

        job_id = self.doc_client.queue_package_build(
            session_id=session_id,
            mode_id=str(project.mode_id or mode_doc["_id"]),
            mode_name=mode_doc.get("name", ""),
            package_id=package.package_id,
            output=output,
            plan=plan_dict,
        )
        self._set_package_job(project, package.package_id, job_id=job_id, status="queued")
        return self._package_to_response(
            self.get_package(session_id, package.package_id),
            mode_doc,
            session_id,
        )

    def build_package_from_intent(
        self,
        mode_doc: Dict[str, Any],
        session_id: str,
        *,
        trade: Optional[str] = None,
        output: str = "pdf",
        filters: Optional[Dict[str, Any]] = None,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        project = self._ensure_project(mode_doc, session_id)
        plan = self.plan_bid_package(project, trade=trade, filters=filters, query=query)
        package = self.build_package(mode_doc, session_id, plan, output)
        return {"plan": plan, "package": package}

    def propose_bid_package(
        self,
        mode_doc: Dict[str, Any],
        session_id: str,
        *,
        trade: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        project = self._ensure_project(mode_doc, session_id)
        all_docs = self._get_project_documents(session_id)
        
        # Determine relevant docs
        relevant_docs = self._filter_documents(all_docs, trade, filters)
        relevant_ids = {doc.file_id for doc in relevant_docs}
        
        # Separate other docs
        other_docs = [doc for doc in all_docs if doc.file_id not in relevant_ids]
        
        return {
            "relevant": [self._document_summary(doc) for doc in relevant_docs],
            "other": [self._document_summary(doc) for doc in other_docs],
            "trade": trade,
            "query": query,
            "filters": filters
        }

    def build_package_from_selection(
        self,
        mode_doc: Dict[str, Any],
        session_id: str,
        file_ids: List[str],
        plan_details: Dict[str, Any],
        output: str = "pdf",
    ) -> Dict[str, Any]:
        self._ensure_project(mode_doc, session_id)
        selected_files = [doc for doc in self._get_project_documents(session_id) if doc.file_id in file_ids]

        if not selected_files:
            raise ValueError("No files selected for the package.")

        trade = plan_details.get("trade")
        query = plan_details.get("query")
        
        plan = self._create_plan_from_documents(selected_files, trade, query)
        
        # Override title if provided in plan_details
        if plan_details.get("package_title"):
            plan["package_title"] = plan_details["package_title"]

        package = self.build_package(mode_doc, session_id, plan, output)
        return package

    def plan_bid_package(
        self,
        project: ProjectContext,
        *,
        trade: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        documents = self._filter_documents(self._get_project_documents(project.session_id or ""), trade, filters)
        if not documents:
            raise ValueError("No documents available for this bid package. Upload project files first.")

        return self._create_plan_from_documents(documents, trade, query)

    def _create_plan_from_documents(
        self, 
        documents: List[DocumentMetadata], 
        trade: Optional[str] = None, 
        query: Optional[str] = None
    ) -> Dict[str, Any]:
        drawings = [doc for doc in documents if doc.is_drawing]
        specs = [doc for doc in documents if doc.is_spec]
        schedules = [doc for doc in documents if "schedule" in [t.lower() for t in doc.topics]]
        other = [doc for doc in documents if doc not in drawings and doc not in specs and doc not in schedules]

        sections: List[Dict[str, Any]] = []
        trade_title = (trade or "General").title()

        def _section(title: str, docs: List[DocumentMetadata]) -> Optional[Dict[str, Any]]:
            if not docs:
                return None
            return {
                "title": title,
                "items": [{"file_id": doc.file_id} for doc in docs],
            }

        for label, docs in [
            (f"{trade_title} Drawings", drawings),
            (f"{trade_title} Specifications", specs),
            ("Schedules & Legends", schedules),
            ("Supporting Documents", other),
        ]:
            section = _section(label, docs)
            if section:
                sections.append(section)

        if not sections:
            raise ValueError("Unable to assemble sections for this package.")

        package_title = query or f"{trade_title} Bid Package"
        return {"package_title": package_title, "sections": sections}

    def get_project_summary(self, session_id: str) -> Optional[Dict[str, Any]]:
        project = self._get_project_by_session(session_id)
        if not project:
            return None
        project_docs = self._get_project_documents(session_id)
        indexing_complete = True
        page_count = 0
        for doc in project_docs:
            extra = getattr(doc, "extra", {}) or {}
            if extra.get("indexing_complete") is False:
                indexing_complete = False
            notes = extra.get("processing_notes") or []
            try:
                if any("truncated" in str(note).lower() for note in notes):
                    indexing_complete = False
            except Exception:  # noqa: BLE001
                pass

            if str(getattr(doc, "file_extension", "")).lower() == "pdf":
                try:
                    page_count += int(extra.get("page_count") or extra.get("page_count_indexed") or 0)
                except Exception:  # noqa: BLE001
                    page_count += 0
            else:
                page_count += 1

        latest_jobs = list(self.jobs.find({"session_id": session_id}).sort("created_at", -1).limit(5))
        packages = [
            self._package_to_response(pkg, {"name": project.mode_name}, session_id)
            for pkg in project.packages[-10:]
        ]

        return {
            "project_id": project.project_id,
            "session_id": project.session_id,
            "mode_id": project.mode_id,
            "mode_name": project.mode_name,
            "status": project.status,
            "file_count": len(project_docs),
            "page_count": page_count,
            "indexing_complete": indexing_complete,
            "package_count": len(project.packages),
            "last_error": project.last_error,
            "last_ingest_job_id": project.last_ingest_job_id,
            "jobs": [
                {
                    "job_id": str(job["_id"]),
                    "job_type": job.get("job_type"),
                    "status": job.get("status"),
                    "progress": job.get("progress", {}),
                    "error": job.get("error"),
                    "created_at": str(job.get("created_at") or ""),
                    "completed_at": str(job.get("completed_at") or ""),
                }
                for job in latest_jobs
            ],
            "files": [
                {
                    "file_id": doc.file_id,
                    "original_filename": doc.original_filename,
                    "trade_tags": doc.trade_tags,
                    "topics": doc.topics,
                    "is_drawing": doc.is_drawing,
                    "is_spec": doc.is_spec,
                    "created_at": doc.created_at,
                }
                for doc in project_docs[-50:]
            ],
            "packages": packages,
            "updated_at": project.updated_at,
        }

    def structured_extract_payload(
        self,
        mode_doc: Dict[str, Any],
        session_id: str,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        project = self._ensure_project(mode_doc, session_id)
        results = self.search(session_id, query, filters or None)
        aggregated = self._aggregate_structured_data(results)
        summary = self._structured_extract_summary(results)
        return {
            "summary": summary,
            "aggregated": aggregated,
            "results": results[:5],
        }

    def generate_assistant_context(self, mode_doc: Dict[str, Any], user_text: str, session_id: str) -> Optional[str]:
        if not self._is_feature_enabled(mode_doc):
            return None

        intent = self.parse_intent(user_text)
        if not intent:
            return None

        project = self._ensure_project(mode_doc, session_id)
        filters = {}
        if intent.get("trade"):
            filters["trade"] = intent["trade"]

        if intent["action"] == "search":
            results = self.search(session_id, intent["query"], filters or None)
            return self._format_search_results(results, intent)

        if intent["action"] == "build_package":
            plan = self.plan_bid_package(project, trade=intent.get("trade"), filters=filters, query=intent.get("query"))
            package = self.build_package(mode_doc, session_id, plan, intent.get("output") or "pdf")
            section_summary = self._summarize_plan(plan)
            if package.get("status") == "ready":
                download_path = package.get("download_url") or self._build_download_path(mode_doc, package, session_id)
                return (
                    f"Built {intent.get('output', 'pdf').upper()} package '{package['title']}'.\n"
                    f"{section_summary}\n"
                    f"Download: [{package['title']} package]({download_path})"
                )
            return (
                f"Started building {intent.get('output', 'pdf').upper()} package '{package['title']}'.\n"
                f"{section_summary}\n"
                "Refresh the document workspace in a moment to download it."
            )

        if intent["action"] == "extract":
            results = self.search(session_id, intent["query"], filters or None)
            summary = self._structured_extract_summary(results)
            if summary:
                return "Structured extraction results:\n" + summary
            return "I could not extract structured data from the current documents."

        return None

    # ------------------------------------------------------------------ #
    # Intent parsing helpers
    # ------------------------------------------------------------------ #
    def parse_intent(self, text: str) -> Optional[Dict[str, Any]]:
        normalized = (text or "").strip().lower()
        if not normalized:
            return None

        action = "answer_question"
        output = None
        filters: Dict[str, Any] = {}

        if any(keyword in normalized for keyword in ["bid package", "package", "combine"]):
            action = "build_package"
        elif any(keyword in normalized for keyword in ["search", "show", "find", "list"]):
            action = "search"
        elif any(keyword in normalized for keyword in ["count", "extract", "how many"]):
            action = "extract"

        if "zip" in normalized:
            output = "zip"
        elif "pdf" in normalized:
            output = "pdf"

        for trade in ["electrical", "mechanical", "civil", "architectural", "structural"]:
            if trade in normalized:
                filters["trade"] = trade
                break

        return {
            "action": action,
            "trade": filters.get("trade"),
            "output": output,
            "filters": filters,
            "query": text,
        }

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def process_ingest_job(
        self,
        payload: Dict[str, Any],
        *,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        session_id = (payload.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("session_id is required for ingest jobs.")

        mode_doc = self._resolve_mode_doc(payload)
        project = self._ensure_project(mode_doc, session_id)
        project.status = "processing"
        project.last_error = None
        project.touch()
        self._save_project(project)

        uploaded_files = list(payload.get("uploaded_files") or [])
        processed_files: List[DocumentMetadata] = []
        errors: List[Dict[str, str]] = []

        with tempfile.TemporaryDirectory(prefix="doc_intel_ingest_", dir=str(self.storage_dir)) as temp_root:
            for index, upload_ref in enumerate(uploaded_files, start=1):
                if progress_callback:
                    progress_callback(
                        {
                            "phase": "processing",
                            "files_total": len(uploaded_files),
                            "files_completed": len(processed_files),
                            "files_failed": len(errors),
                            "current_file": upload_ref.get("original_filename"),
                            "current_index": index,
                        }
                    )
                try:
                    local_path = self.storage.download_to_local(upload_ref, work_dir=temp_root)
                    processed_files.extend(
                        self._process_path(
                            local_path,
                            mode_doc,
                            project,
                            session_id=session_id,
                            source_ref=upload_ref,
                            zip_depth=0,
                        )
                    )
                    if self._is_zip_ref(upload_ref):
                        self.storage.delete(upload_ref)
                except Exception as exc:  # noqa: BLE001
                    errors.append(
                        {
                            "file": upload_ref.get("original_filename") or "unknown",
                            "error": str(exc),
                        }
                    )

        project.file_count = self.documents.count_documents({"session_id": session_id})
        project.status = "ready" if project.file_count else "failed"
        project.last_error = errors[0]["error"] if errors and not project.file_count else None
        project.touch()
        self._save_project(project)

        return {
            "ingested": len(processed_files),
            "files_total": len(uploaded_files),
            "files": [self._document_summary(doc) for doc in processed_files],
            "errors": errors,
        }

    def process_package_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session_id = (payload.get("session_id") or "").strip()
        package_id = (payload.get("package_id") or "").strip()
        output = (payload.get("output") or "pdf").lower()
        if output not in {"pdf", "zip"}:
            raise ValueError("output must be 'pdf' or 'zip'")

        project = self._get_project_by_session(session_id)
        if not project:
            raise ValueError("No project context found for mode.")

        plan = payload.get("plan") or {}
        if not plan:
            raise ValueError("plan is required for build_package jobs.")

        with tempfile.TemporaryDirectory(prefix="doc_intel_package_", dir=str(self.storage_dir)) as temp_root:
            local_plan = self._materialize_plan_sources(session_id, plan, temp_root)
            if output == "zip":
                package_path = self.toolbox.build_zip_package(local_plan, str(self.storage_dir / "packages"))
            else:
                package_path = self.toolbox.build_pdf_package(local_plan, str(self.storage_dir / "packages"))

            artifact = self.storage.upload_local_artifact(
                package_path,
                session_id=session_id,
                prefix="packages",
                download_filename=Path(package_path).name,
            )

        package = self._set_package_artifact(
            project,
            package_id=package_id,
            output=output,
            artifact=artifact,
        )
        return self._package_to_response(package, {"name": project.mode_name}, session_id)

    def mark_project_failed(self, session_id: Optional[str], error: str) -> None:
        if not session_id:
            return
        project = self._get_project_by_session(session_id)
        if not project:
            return
        project.status = "failed"
        project.last_error = error
        project.touch()
        self._save_project(project)

    def mark_package_failed(self, session_id: Optional[str], package_id: Optional[str], error: str, *, job_id: Optional[str] = None) -> None:
        if not session_id or not package_id:
            return
        project = self._get_project_by_session(session_id)
        if not project:
            return
        self._set_package_job(project, package_id, job_id=job_id, status="failed", error=error)

    def _process_path(
        self,
        file_path: str,
        mode_doc: Dict[str, Any],
        project: ProjectContext,
        *,
        session_id: str,
        source_ref: Optional[Dict[str, Any]] = None,
        zip_depth: int = 0,
    ) -> List[DocumentMetadata]:
        file_info = self.toolbox.detect_file_type(file_path)
        logger.debug(f"Detected file type for {file_path}: {file_info}")
        processed: List[DocumentMetadata] = []

        if file_info.get("file_extension") == "zip":
            if zip_depth >= self.max_zip_depth:
                raise ValueError(f"ZIP nesting exceeds the limit of {self.max_zip_depth}.")

            logger.info(f"Extracting ZIP: {file_path}")
            try:
                for extracted_path in self.toolbox.iter_extract_zip(
                    file_path,
                    max_members=self.max_zip_members,
                    max_member_size_bytes=self.max_zip_member_size_bytes,
                    max_total_size_bytes=self.max_zip_total_size_bytes,
                    max_compression_ratio=self.max_zip_compression_ratio,
                ):
                    processed.extend(
                        self._process_path(
                            extracted_path,
                            mode_doc,
                            project,
                            session_id=session_id,
                            source_ref=None,
                            zip_depth=zip_depth + 1,
                        )
                    )
                    self._cleanup_file_and_parent(extracted_path)
            except ZipExtractionError as exc:
                raise ValueError(str(exc)) from exc
            self._cleanup_file_and_parent(file_path)
            return processed

        text_payload = ""
        ocr_text = ""
        extra = {"processing_notes": [], "indexing_complete": True}

        if file_info.get("is_pdf"):
            logger.info(f"Parsing PDF: {file_path}")
            parsed = self.toolbox.parse_pdf(file_path, max_seconds=self.max_pdf_seconds)
            text_payload = self._limit_text(parsed.get("text", ""))
            extra["images"] = parsed.get("images", [])
            extra["page_count"] = parsed.get("page_count") or 0
            if parsed.get("truncated"):
                extra["indexing_complete"] = False
                extra["processing_notes"].append(
                    f"PDF parsing truncated (time limit hit: seconds<={self.max_pdf_seconds})."
                )
        elif file_info.get("is_image"):
            self._guard_image_size(file_path)
            logger.info(f"Processing Image (OCR): {file_path}")
            enhanced_path = self.toolbox.enhance_blueprint_for_ocr(file_path)
            try:
                ocr_text = self._limit_text(self.toolbox.ocr_image(enhanced_path))
            finally:
                if enhanced_path and enhanced_path != file_path and os.path.exists(enhanced_path):
                    try:
                        os.remove(enhanced_path)
                    except Exception as e:
                        logger.warning(f"Failed to remove temp OCR file {enhanced_path}: {e}")
            text_payload = ocr_text
        else:
            logger.info(f"Reading text file: {file_path}")
            text_payload = self._read_text_file(file_path)

        storage_ref = source_ref or self.storage.upload_local_artifact(
            file_path,
            session_id=session_id,
            prefix="sources",
            download_filename=Path(file_path).name,
        )

        analysis_text = self._analysis_text(text_payload, ocr_text, Path(file_path).name)
        logger.info(f"Classifying document: {file_path}")
        classification = self.toolbox.classify_document(analysis_text, Path(file_path).name, ocr_text)
        embedding = self.toolbox.embed_text(analysis_text[: self.max_embed_chars])

        metadata = DocumentMetadata(
            file_id=str(uuid4()),
            file_path=storage_ref.get("file_path", ""),
            original_filename=Path(file_path).name,
            mime_type=file_info.get("mime_type", "application/octet-stream"),
            file_extension=file_info.get("file_extension", ""),
            session_id=session_id,
            project_id=project.project_id,
            storage_bucket=storage_ref.get("storage_bucket"),
            storage_key=storage_ref.get("storage_key"),
            file_size_bytes=storage_ref.get("file_size_bytes"),
            trade_tags=classification.get("trade_tags", []),
            division_tags=classification.get("division_tags", []),
            topics=classification.get("topics", []),
            is_drawing=classification.get("is_drawing", False),
            is_spec=classification.get("is_spec", False),
            raw_text=text_payload[: self.max_excerpt_chars],
            ocr_text=ocr_text[: self.max_excerpt_chars],
            search_text=self._search_text(text_payload, ocr_text),
            embedding_id=embedding.get("embedding_id"),
            embedding=None,
            checksum=file_info.get("checksum"),
            extra=extra,
        )

        self._upsert_document(project, metadata)
        processed.append(metadata)
        logger.info(f"Document processed and upserted: {metadata.file_id}")
        return processed

    def _upsert_document(self, project: ProjectContext, document: DocumentMetadata) -> None:
        existing = None
        if document.checksum:
            existing = self.documents.find_one(
                {"session_id": project.session_id, "checksum": document.checksum}
            )
        if existing:
            document.file_id = existing.get("file_id") or document.file_id
        payload = document.to_dict()
        payload["updated_at"] = datetime.utcnow().isoformat()
        self.documents.update_one(
            {"session_id": project.session_id, "file_id": document.file_id},
            {"$set": payload},
            upsert=True,
        )
        project.file_count = self.documents.count_documents({"session_id": project.session_id})
        project.touch()
        self._save_project(project)

    def _ensure_project(self, mode_doc: Dict[str, Any], session_id: str) -> ProjectContext:
        project = self._get_project_by_session(session_id)
        if project:
            return project

        context = ProjectContext(
            session_id=session_id,
            mode_id=str(mode_doc["_id"]),
            mode_name=mode_doc.get("name"),
            settings=mode_doc.get("doc_intelligence_settings", {}),
            status="idle",
        )
        self._save_project(context)
        return context

    def _save_project(self, context: ProjectContext) -> None:
        payload = context.to_dict()
        payload["files"] = []
        payload["file_count"] = context.file_count or self.documents.count_documents({"session_id": context.session_id})
        self.projects.update_one({"session_id": context.session_id}, {"$set": payload}, upsert=True)

    def _get_project_by_session(self, session_id: str) -> Optional[ProjectContext]:
        doc = self.projects.find_one({"session_id": session_id})
        if not doc:
            return None
        return ProjectContext.from_dict(doc)

    def _read_text_file(self, file_path: str) -> str:
        try:
            file_size = os.path.getsize(file_path)
            if file_size > self.max_text_file_bytes:
                raise ValueError(
                    f"Text file exceeds the {self.max_text_file_bytes} byte processing limit."
                )
            with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
                return self._limit_text(handle.read())
        except Exception:  # noqa: BLE001
            return ""

    def _is_feature_enabled(self, mode_doc: Dict[str, Any]) -> bool:
        settings = mode_doc.get("doc_intelligence_settings", {})
        return bool(mode_doc.get("doc_intelligence_enabled") or settings.get("enabled"))

    def get_package(self, session_id: str, package_id: str) -> Optional[BidPackage]:
        project = self._get_project_by_session(session_id)
        if not project:
            return None
        for package in project.packages:
            if package.package_id == package_id:
                return package
        return None

    def _format_search_results(self, results: List[Dict[str, Any]], intent: Dict[str, Any]) -> str:
        if not results:
            return "No matching documents found for your request."
        top_lines = []
        for hit in results[:5]:
            document = hit["document"]
            trades = ", ".join(document.get("trade_tags", [])) or "unlabeled"
            source_type = "Drawing" if document.get("is_drawing") else "Specification" if document.get("is_spec") else "Document"
            line = f"- {document.get('original_filename')} [{source_type}, trades: {trades}]"
            top_lines.append(line)
        return "Relevant documents:\n" + "\n".join(top_lines)

    def _structured_extract_summary(self, results: List[Dict[str, Any]]) -> Optional[str]:
        aggregated = self._aggregate_structured_data(results)
        if not aggregated:
            return None
        lines = []
        for bucket, entries in aggregated.items():
            pretty_bucket = bucket.replace("_", " ").title()
            parts = [f"{label} x{count}" for label, count in entries.items()]
            lines.append(f"{pretty_bucket}: {', '.join(parts)}")
        return "\n".join(lines)

    def _aggregate_structured_data(self, results: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
        aggregated: Dict[str, Dict[str, int]] = {}
        for hit in results[:5]:
            document = hit.get("document", {})
            extraction = self.toolbox.structured_extract(
                document.get("search_text") or document.get("raw_text", ""),
                document.get("ocr_text"),
            )
            for bucket, entries in extraction.items():
                bucket_store = aggregated.setdefault(bucket, {})
                for label, count in entries.items():
                    bucket_store[label] = bucket_store.get(label, 0) + count
        return aggregated

    def _filter_documents(
        self,
        documents: List[DocumentMetadata],
        trade: Optional[str],
        filters: Optional[Dict[str, Any]],
    ) -> List[DocumentMetadata]:
        if not trade and not filters:
            return documents
        filtered: List[DocumentMetadata] = []
        for doc in documents:
            if trade and trade not in [t.lower() for t in doc.trade_tags]:
                continue
            filtered.append(doc)
        return filtered or documents

    def _summarize_plan(self, plan: Dict[str, Any]) -> str:
        lines = []
        for section in plan.get("sections", []):
            lines.append(f"{section.get('title')}: {len(section.get('items', []))} file(s)")
        return "\n".join(lines)

    def _build_download_path(self, mode_doc: Dict[str, Any], package: Dict[str, Any], session_id: str) -> str:
        mode_name = mode_doc.get("name", "")
        file_type = package.get("file_type") or ("zip" if package.get("output_zip_path") else "pdf")
        return (
            f"/flask/doc-intel/package/{package.get('package_id')}?"
            f"mode={mode_name}&session_id={session_id}&file_type={file_type}"
        )

    def _cleanup_file_and_parent(self, file_path: str) -> None:
        """Best-effort removal of a file and its empty parent directory (within storage)."""
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            parent = Path(file_path).parent
            if (
                parent
                and parent != self.storage_dir
                and parent.exists()
                and parent.is_dir()
                and not any(parent.iterdir())
            ):
                parent.rmdir()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Temp cleanup failed for {file_path}: {exc}")

    def _get_project_documents(self, session_id: str) -> List[DocumentMetadata]:
        docs = [
            DocumentMetadata.from_dict(doc)
            for doc in self.documents.find({"session_id": session_id}).sort("created_at", 1)
        ]
        if docs:
            return docs
        project = self._get_project_by_session(session_id)
        return list(project.files) if project else []

    def _resolve_mode_doc(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        mode_id = payload.get("mode_id")
        mode_name = payload.get("mode_name")
        mode_doc = None
        if mode_id:
            mode_doc = self.modes.find_one({"_id": mode_id}) or self.modes.find_one({"_id": str(mode_id)})
            if not mode_doc:
                try:
                    mode_doc = self.modes.find_one({"_id": ObjectId(str(mode_id))})
                except Exception:  # noqa: BLE001
                    mode_doc = None
        if not mode_doc and mode_name:
            mode_doc = self.modes.find_one({"name": mode_name})
        if not mode_doc:
            raise ValueError("Unable to resolve mode for doc-intel job.")
        return mode_doc

    def _limit_text(self, text: str) -> str:
        value = (text or "").strip()
        if len(value) <= self.max_text_chars:
            return value
        return value[: self.max_text_chars]

    def _search_text(self, text_payload: str, ocr_text: str) -> str:
        combined = "\n".join(part for part in [text_payload, ocr_text] if part).strip()
        return combined[: self.max_text_chars]

    def _analysis_text(self, text_payload: str, ocr_text: str, filename: str) -> str:
        combined = " ".join(part for part in [text_payload, ocr_text, filename] if part).strip()
        return combined[: self.max_text_chars]

    def _document_summary(self, document: DocumentMetadata) -> Dict[str, Any]:
        return {
            "file_id": document.file_id,
            "original_filename": document.original_filename,
            "mime_type": document.mime_type,
            "file_extension": document.file_extension,
            "trade_tags": document.trade_tags,
            "topics": document.topics,
            "is_drawing": document.is_drawing,
            "is_spec": document.is_spec,
            "created_at": document.created_at,
        }

    def _materialize_plan_sources(self, session_id: str, plan: Dict[str, Any], temp_root: str) -> Dict[str, Any]:
        documents = {doc.file_id: doc for doc in self._get_project_documents(session_id)}
        sections: List[Dict[str, Any]] = []
        for section in plan.get("sections", []):
            materialized_items: List[Dict[str, Any]] = []
            for item in section.get("items", []):
                doc = documents.get(item.get("file_id"))
                if doc:
                    local_path = self.storage.download_to_local(doc.to_dict(), work_dir=temp_root)
                    new_item = dict(item)
                    new_item["source_file"] = local_path
                    materialized_items.append(new_item)
                    continue
                source_file = item.get("source_file")
                if source_file and Path(source_file).exists():
                    materialized_items.append(dict(item))
            if materialized_items:
                sections.append({"title": section.get("title"), "items": materialized_items})
        return {"package_title": plan.get("package_title", "Bid Package"), "sections": sections}

    def _set_package_job(
        self,
        project: ProjectContext,
        package_id: str,
        *,
        job_id: Optional[str],
        status: str,
        error: Optional[str] = None,
    ) -> None:
        for package in project.packages:
            if package.package_id == package_id:
                package.job_id = job_id
                package.status = status
                package.error = error
                break
        project.touch()
        self._save_project(project)

    def _set_package_artifact(
        self,
        project: ProjectContext,
        *,
        package_id: str,
        output: str,
        artifact: Dict[str, Any],
    ) -> BidPackage:
        for package in project.packages:
            if package.package_id != package_id:
                continue
            package.storage_bucket = artifact.get("storage_bucket")
            package.storage_key = artifact.get("storage_key")
            package.file_type = output
            package.status = "ready"
            package.error = None
            if output == "zip":
                package.output_zip_path = artifact.get("file_path", "")
                package.output_pdf_path = None
            else:
                package.output_pdf_path = artifact.get("file_path", "")
                package.output_zip_path = None
            project.status = "ready"
            project.touch()
            self._save_project(project)
            return package
        raise ValueError("Package not found while updating artifact metadata.")

    def _package_to_response(self, package: Optional[BidPackage], mode_doc: Optional[Dict[str, Any]], session_id: str) -> Dict[str, Any]:
        if not package:
            raise ValueError("Package not found")
        payload = package.to_dict()
        if package.status == "ready":
            payload["download_url"] = self._build_download_path(mode_doc or {}, payload, session_id)
            presigned = self.storage.build_download_url(payload, download_filename=self._package_filename(package))
            if presigned:
                payload["presigned_download_url"] = presigned
        return payload

    def _package_filename(self, package: BidPackage) -> str:
        ext = ".zip" if (package.file_type or "") == "zip" else ".pdf"
        title = package.title or "package"
        safe = "".join(ch if ch.isalnum() or ch in {" ", "-", "_"} else "_" for ch in title).strip()
        return f"{safe or 'package'}{ext}"

    def _is_zip_ref(self, ref: Dict[str, Any]) -> bool:
        name = (ref.get("original_filename") or "").lower()
        return name.endswith(".zip")

    def _guard_image_size(self, file_path: str) -> None:
        try:
            from PIL import Image

            with Image.open(file_path) as img:
                width, height = img.size
                if (width * height) > self.max_image_pixels:
                    raise ValueError(
                        f"Image exceeds the {self.max_image_pixels} pixel processing limit."
                    )
        except ValueError:
            raise
        except Exception:
            return

