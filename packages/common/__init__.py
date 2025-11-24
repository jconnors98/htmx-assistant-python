"""
Common shared packages between the main Flask app and the scraper worker.

This package intentionally keeps dependencies minimal so it can be imported
by both services without dragging in heavy scraping requirements.
"""

__all__ = ["scraper_contracts"]

