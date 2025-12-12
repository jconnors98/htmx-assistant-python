from __future__ import annotations

import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from models.metadata import BidPackage, DocumentMetadata, ProjectContext, Section
from tools import DocumentToolbox

import logging

logger = logging.getLogger(__name__)


class DocumentIntelligenceService:
    """
    High-level coordinator that wires the document toolset into MongoDB-backed
    project contexts per mode.
    """

    def __init__(
        self,
        *,
        modes_collection,
        projects_collection,
        storage_dir: str,
        toolbox: DocumentToolbox,
        expiry_minutes: int = 30,
    ):
        self.modes = modes_collection
        self.projects = projects_collection
        self.toolbox = toolbox
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.expiry_minutes = expiry_minutes

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def cleanup_expired_documents(self) -> int:
        """
        Deletes documents and packages that have exceeded their expiry time.
        Returns the number of cleaned up projects.
        """
        expiry_threshold = datetime.utcnow() - timedelta(minutes=self.expiry_minutes)
        projects = self.projects.find({"updated_at": {"$lt": expiry_threshold.isoformat()}})
        
        cleaned_count = 0
        for doc in projects:
            project = ProjectContext.from_dict(doc)
            self._delete_project_files(project)
            self.projects.delete_one({"session_id": project.session_id})
            cleaned_count += 1
            
        if cleaned_count > 0:
            logger.info(f"DocIntel: cleaned up {cleaned_count} expired projects.")
            
        return cleaned_count

    def _delete_project_files(self, project: ProjectContext) -> None:
        # Delete document files
        for document in project.files:
            try:
                if document.file_path and os.path.exists(document.file_path):
                    # Also try to remove the parent directory if it was created for this upload
                    parent_dir = Path(document.file_path).parent
                    if parent_dir.exists() and parent_dir.is_dir() and str(self.storage_dir) in str(parent_dir):
                         shutil.rmtree(parent_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Error deleting file {document.file_path}: {e}")

        # Delete package files
        for package in project.packages:
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

        logger.info(f"Starting ingestion for session {session_id}")
        processed_files = []
        for file_storage in files:
            fname = getattr(file_storage, "filename", "unknown")
            if not getattr(file_storage, "filename", None):
                continue
            
            try:
                logger.info(f"Persisting upload: {fname}")
                saved_path = self._persist_upload(file_storage)
                logger.info(f"Processing path: {saved_path}")
                processed_files.extend(self._process_path(saved_path, mode_doc, project))
            except Exception as e:
                logger.error(f"Error processing file {fname}: {e}", exc_info=True)
                # Continue with other files? Or raise? 
                # Current logic implies we might want to continue or just let it bubble up.
                # Given the loop, maybe we should continue but for now let's just log and re-raise or let it bubble if critical.
                # The original code didn't catch here, so I won't suppress it, just log.
                raise

        project.touch()
        self._save_project(project)
        
        logger.info(f"Ingestion finished. Processed {len(processed_files)} documents.")

        return {
            "ingested": len(processed_files),
            "files": [doc.to_dict() for doc in processed_files],
        }

    def search(self, session_id: str, query: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        project = self._get_project_by_session(session_id)
        if not project:
            return []
        documents = [doc.to_dict() for doc in project.files]
        return self.toolbox.search_documents(query, documents, filters=filters)

    def build_package(self, mode_doc: Dict[str, Any], session_id: str, plan_dict: Dict[str, Any], output: str = "pdf") -> Dict[str, Any]:
        project = self._get_project_by_session(session_id)
        if not project:
            raise ValueError("No project context found for mode.")

        logger.info(
            "DocIntel: building package",
            extra={
                "session_id": session_id,
                "mode_id": mode_doc.get("_id"),
                "output": output,
                "package_title": plan_dict.get("package_title"),
                "section_count": len(plan_dict.get("sections", [])),
            },
        )

        if output == "zip":
            zip_path = self.toolbox.build_zip_package(plan_dict, str(self.storage_dir))
            package = BidPackage(
                title=plan_dict.get("package_title", "Bid Package"),
                sections=[Section.from_dict(section) for section in plan_dict.get("sections", [])],
                output_zip_path=zip_path,
            )
        else:
            pdf_path = self.toolbox.build_pdf_package(plan_dict, str(self.storage_dir))
            package = BidPackage(
                title=plan_dict.get("package_title", "Bid Package"),
                sections=[Section.from_dict(section) for section in plan_dict.get("sections", [])],
                output_pdf_path=pdf_path,
            )

        project.packages.append(package)
        project.touch()
        self._save_project(project)

        logger.info(
            "DocIntel: package built",
            extra={
                "session_id": session_id,
                "mode_id": mode_doc.get("_id"),
                "package_id": package.package_id,
                "output_pdf_path": package.output_pdf_path,
                "output_zip_path": package.output_zip_path,
                "section_count": len(package.sections),
            },
        )

        return package.to_dict()

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
        all_docs = project.files
        
        # Determine relevant docs
        relevant_docs = self._filter_documents(all_docs, trade, filters)
        relevant_ids = {doc.file_id for doc in relevant_docs}
        
        # Separate other docs
        other_docs = [doc for doc in all_docs if doc.file_id not in relevant_ids]
        
        return {
            "relevant": [doc.to_dict() for doc in relevant_docs],
            "other": [doc.to_dict() for doc in other_docs],
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
        project = self._ensure_project(mode_doc, session_id)
        
        # Filter files by ID
        selected_files = [doc for doc in project.files if doc.file_id in file_ids]
        
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
        documents = self._filter_documents(project.files, trade, filters)
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
                "items": [{"source_file": doc.file_path} for doc in docs],
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
        return {
            "project_id": project.project_id,
            "session_id": project.session_id,
            "mode_id": project.mode_id,
            "mode_name": project.mode_name,
            "file_count": len(project.files),
            "package_count": len(project.packages),
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
                for doc in project.files[-50:]
            ],
            "packages": [pkg.to_dict() for pkg in project.packages[-10:]],
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
            download_path = self._build_download_path(mode_doc, package, session_id)
            section_summary = self._summarize_plan(plan)
            return (
                f"📦 Built {intent.get('output', 'pdf').upper()} package '{package['title']}'.\n"
                f"{section_summary}\n"
                f"Download: [{package['title']} package]({download_path})"
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
    def _process_path(self, file_path: str, mode_doc: Dict[str, Any], project: ProjectContext) -> List[DocumentMetadata]:
        file_info = self.toolbox.detect_file_type(file_path)
        logger.debug(f"Detected file type for {file_path}: {file_info}")
        processed: List[DocumentMetadata] = []

        if file_info.get("file_extension") == "zip":
            logger.info(f"Extracting ZIP: {file_path}")
            extracted = self.toolbox.extract_zip(file_path)
            logger.info(f"Extracted {len(extracted)} files from ZIP")
            for extracted_path in extracted:
                processed.extend(self._process_path(extracted_path, mode_doc, project))
            # Original ZIP is no longer needed once extracted; remove to avoid temp bloat
            self._cleanup_file_and_parent(file_path)
            return processed

        text_payload = ""
        ocr_text = ""
        extra = {"processing_notes": []}

        if file_info.get("is_pdf"):
            logger.info(f"Parsing PDF: {file_path}")
            parsed = self.toolbox.parse_pdf(file_path)
            text_payload = parsed.get("text", "")
            extra["images"] = parsed.get("images", [])
        elif file_info.get("is_image"):
            logger.info(f"Processing Image (OCR): {file_path}")
            enhanced_path = self.toolbox.enhance_blueprint_for_ocr(file_path)
            try:
                ocr_text = self.toolbox.ocr_image(enhanced_path)
            finally:
                # Cleanup intermediate enhanced file if it was created
                if enhanced_path and enhanced_path != file_path and os.path.exists(enhanced_path):
                    try:
                        os.remove(enhanced_path)
                    except Exception as e:
                        logger.warning(f"Failed to remove temp OCR file {enhanced_path}: {e}")
            text_payload = ocr_text
        else:
            logger.info(f"Reading text file: {file_path}")
            text_payload = self._read_text_file(file_path)

        logger.info(f"Classifying document: {file_path}")
        classification = self.toolbox.classify_document(text_payload, Path(file_path).name, ocr_text)
        embedding = self.toolbox.embed_text(text_payload or ocr_text)

        metadata = DocumentMetadata(
            file_id=str(uuid4()),
            file_path=file_path,
            original_filename=Path(file_path).name,
            mime_type=file_info.get("mime_type", "application/octet-stream"),
            file_extension=file_info.get("file_extension", ""),
            trade_tags=classification.get("trade_tags", []),
            division_tags=classification.get("division_tags", []),
            topics=classification.get("topics", []),
            is_drawing=classification.get("is_drawing", False),
            is_spec=classification.get("is_spec", False),
            raw_text=text_payload,
            ocr_text=ocr_text,
            embedding_id=embedding.get("embedding_id"),
            embedding=embedding.get("embedding"),
            checksum=file_info.get("checksum"),
            extra=extra,
        )

        self._upsert_document(project, metadata)
        processed.append(metadata)
        logger.info(f"Document processed and upserted: {metadata.file_id}")
        return processed

    def _upsert_document(self, project: ProjectContext, document: DocumentMetadata) -> None:
        for index, existing in enumerate(project.files):
            if existing.checksum and document.checksum and existing.checksum == document.checksum:
                project.files[index] = document
                break
        else:
            project.files.append(document)

    def _ensure_project(self, mode_doc: Dict[str, Any], session_id: str) -> ProjectContext:
        project = self._get_project_by_session(session_id)
        if project:
            return project

        context = ProjectContext(
            session_id=session_id,
            mode_id=str(mode_doc["_id"]),
            mode_name=mode_doc.get("name"),
            settings=mode_doc.get("doc_intelligence_settings", {}),
        )
        self._save_project(context)
        return context

    def _save_project(self, context: ProjectContext) -> None:
        self.projects.update_one(
            {"session_id": context.session_id},
            {"$set": context.to_dict()},
            upsert=True,
        )

    def _get_project_by_session(self, session_id: str) -> Optional[ProjectContext]:
        doc = self.projects.find_one({"session_id": session_id})
        if not doc:
            return None
        return ProjectContext.from_dict(doc)

    def _persist_upload(self, file_storage) -> str:
        sanitized_name = Path(file_storage.filename).name
        destination_dir = self.storage_dir / uuid4().hex
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / sanitized_name
        file_storage.save(destination)  # type: ignore[attr-defined]
        return str(destination)

    def _read_text_file(self, file_path: str) -> str:
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
                return handle.read()
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
            extraction = self.toolbox.structured_extract(document.get("raw_text", ""), document.get("ocr_text"))
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
        file_type = "zip" if package.get("output_zip_path") else "pdf"
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

