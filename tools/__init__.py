from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from . import classifier, embeddings, extract, ocr, package_builder, pdf_tools, search


class DocumentToolbox:
    """
    Convenience wrapper that exposes all document intelligence helper functions
    through a single dependency-injected interface.
    """

    def __init__(self, *, openai_client=None, storage_dir: Optional[str] = None):
        self.client = openai_client
        self.storage_dir = Path(storage_dir or "./storage/doc_intel")
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    # Extraction utilities -------------------------------------------------
    def extract_zip(self, zip_file_path: str, output_dir: Optional[str] = None) -> List[str]:
        if not output_dir:
            import tempfile
            # Create a dedicated temp directory within storage_dir to ensure it uses the main volume
            # and can be cleaned up if needed.
            tmp_root = self.storage_dir / "tmp"
            tmp_root.mkdir(exist_ok=True)
            output_dir = tempfile.mkdtemp(prefix="zip_extract_", dir=str(tmp_root))
            
        return extract.extract_zip(zip_file_path, output_dir)

    def detect_file_type(self, file_path: str) -> Dict[str, Any]:
        return extract.detect_file_type(file_path)

    # PDF & OCR ------------------------------------------------------------
    def parse_pdf(self, file_path: str) -> Dict[str, Any]:
        return pdf_tools.parse_pdf(file_path)

    def enhance_blueprint_for_ocr(self, image_path: str) -> str:
        # Create a tmp directory in storage_dir for these intermediate files
        tmp_root = self.storage_dir / "tmp" 
        tmp_root.mkdir(exist_ok=True)
        return ocr.enhance_blueprint_for_ocr(image_path, work_dir=str(tmp_root))

    def ocr_image(self, image_path: str) -> str:
        return ocr.ocr_image(image_path)

    # Classification & extraction ------------------------------------------
    def classify_document(self, file_text: str, filename: str, ocr_text: Optional[str] = None) -> Dict[str, Any]:
        return classifier.classify_document(file_text, filename, ocr_text)

    def structured_extract(self, file_text: str, ocr_text: Optional[str] = None) -> Dict[str, Any]:
        return classifier.structured_extract(file_text, ocr_text)

    # Embeddings -----------------------------------------------------------
    def embed_text(self, text: str) -> Dict[str, Any]:
        return embeddings.embed_text(text, client=self.client)

    # Search ----------------------------------------------------------------
    def search_documents(self, query: str, documents: List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return search.search_documents(query, documents, filters=filters)

    # Package builders ------------------------------------------------------
    def build_pdf_package(self, plan_dict: Dict[str, Any], output_dir: Optional[str] = None) -> str:
        return package_builder.build_pdf_package(plan_dict, output_dir or str(self.storage_dir))

    def build_zip_package(self, plan_dict: Dict[str, Any], output_dir: Optional[str] = None) -> str:
        return package_builder.build_zip_package(plan_dict, output_dir or str(self.storage_dir))

