"""Service adapters used by the Flask application."""

from .doc_intel_client import DocumentIntelligenceClient, DocumentIntelligenceClientMode
from .scraper_client import ScraperClient, ScraperClientMode

__all__ = [
    "DocumentIntelligenceClient",
    "DocumentIntelligenceClientMode",
    "ScraperClient",
    "ScraperClientMode",
]

