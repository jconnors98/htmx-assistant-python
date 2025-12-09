from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


@dataclass
class Section:
    title: str
    items: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "items": self.items,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Section":
        return Section(
            title=data.get("title", "Untitled Section"),
            items=list(data.get("items", [])),
        )


@dataclass
class DocumentMetadata:
    file_id: str
    file_path: str
    original_filename: str
    mime_type: str
    file_extension: str
    trade_tags: List[str] = field(default_factory=list)
    division_tags: List[int] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)
    is_drawing: bool = False
    is_spec: bool = False
    ocr_text: str = ""
    raw_text: str = ""
    embedding_id: Optional[str] = None
    embedding: Optional[List[float]] = None
    checksum: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "DocumentMetadata":
        return DocumentMetadata(
            file_id=data.get("file_id"),
            file_path=data.get("file_path"),
            original_filename=data.get("original_filename", ""),
            mime_type=data.get("mime_type", "application/octet-stream"),
            file_extension=data.get("file_extension", ""),
            trade_tags=list(data.get("trade_tags", [])),
            division_tags=list(data.get("division_tags", [])),
            topics=list(data.get("topics", [])),
            is_drawing=bool(data.get("is_drawing", False)),
            is_spec=bool(data.get("is_spec", False)),
            ocr_text=data.get("ocr_text", ""),
            raw_text=data.get("raw_text", ""),
            embedding_id=data.get("embedding_id"),
            embedding=data.get("embedding"),
            checksum=data.get("checksum"),
            extra=dict(data.get("extra", {})),
            created_at=data.get("created_at", _now_iso()),
        )


@dataclass
class BidPackage:
    package_id: str = field(default_factory=lambda: str(uuid4()))
    title: str = "Untitled Package"
    sections: List[Section] = field(default_factory=list)
    output_pdf_path: Optional[str] = None
    output_zip_path: Optional[str] = None
    created_at: str = field(default_factory=_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "package_id": self.package_id,
            "title": self.title,
            "sections": [section.to_dict() for section in self.sections],
            "output_pdf_path": self.output_pdf_path,
            "output_zip_path": self.output_zip_path,
            "created_at": self.created_at,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "BidPackage":
        return BidPackage(
            package_id=data.get("package_id", str(uuid4())),
            title=data.get("title", "Untitled Package"),
            sections=[Section.from_dict(section) for section in data.get("sections", [])],
            output_pdf_path=data.get("output_pdf_path"),
            output_zip_path=data.get("output_zip_path"),
            created_at=data.get("created_at", _now_iso()),
        )


@dataclass
class ProjectContext:
    project_id: str = field(default_factory=lambda: str(uuid4()))
    session_id: Optional[str] = None
    mode_id: Optional[str] = None
    mode_name: Optional[str] = None
    files: List[DocumentMetadata] = field(default_factory=list)
    vector_index_name: Optional[str] = None
    packages: List[BidPackage] = field(default_factory=list)
    settings: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_id": self.project_id,
            "session_id": self.session_id,
            "mode_id": self.mode_id,
            "mode_name": self.mode_name,
            "files": [doc.to_dict() for doc in self.files],
            "vector_index_name": self.vector_index_name,
            "packages": [pkg.to_dict() for pkg in self.packages],
            "settings": self.settings,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ProjectContext":
        return ProjectContext(
            project_id=data.get("project_id", str(uuid4())),
            session_id=data.get("session_id"),
            mode_id=data.get("mode_id"),
            mode_name=data.get("mode_name"),
            files=[DocumentMetadata.from_dict(doc) for doc in data.get("files", [])],
            vector_index_name=data.get("vector_index_name"),
            packages=[BidPackage.from_dict(pkg) for pkg in data.get("packages", [])],
            settings=dict(data.get("settings", {})),
            created_at=data.get("created_at", _now_iso()),
            updated_at=data.get("updated_at", _now_iso()),
        )

    def touch(self) -> None:
        self.updated_at = _now_iso()

