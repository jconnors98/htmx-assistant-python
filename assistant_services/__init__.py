"""Service adapters used by the Flask application."""

from .scraper_client import ScraperClient, ScraperClientMode

__all__ = ["ScraperClient", "ScraperClientMode"]

