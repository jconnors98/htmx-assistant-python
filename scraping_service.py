"""
Web scraping service for extracting content from configured websites.
Uses Playwright for dynamic content handling (accordions, tabs, etc.)
"""

import atexit
import io
import os
import queue
import re
import sys
import threading
import time
import tempfile
import xml.etree.ElementTree as ET
from collections import deque
from contextlib import contextmanager, nullcontext
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse
from decouple import config

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

try:
    import psutil  # Optional, used for instrumentation
except ImportError:  # pragma: no cover
    psutil = None


class BrowserPool:
    """Thread-safe pool for sharing limited Playwright browser instances."""

    def __init__(self, start_playwright_fn, max_browsers: int = 2):
        self._start_playwright_fn = start_playwright_fn
        self._max_browsers = max(1, max_browsers)
        self._lock = threading.Lock()
        self._available_browsers: "queue.Queue" = queue.Queue()
        self._playwright = None
        self._created_browsers = 0

    def _ensure_playwright(self):
        if not self._playwright:
            self._playwright = self._start_playwright_fn()

    def acquire(self, timeout: Optional[float] = None):
        """Acquire a browser, blocking if necessary."""
        try:
            browser = self._available_browsers.get_nowait()
            if browser.is_connected():
                return browser
            browser.close()
        except queue.Empty:
            pass

        with self._lock:
            if self._created_browsers < self._max_browsers:
                self._ensure_playwright()
                browser = self._playwright.chromium.launch(headless=True)
                self._created_browsers += 1
                return browser

        browser = self._available_browsers.get(timeout=timeout)
        return browser

    def release(self, browser):
        """Return a browser to the pool."""
        if browser and browser.is_connected():
            self._available_browsers.put(browser)
        else:
            with self._lock:
                self._created_browsers = max(0, self._created_browsers - 1)

    def shutdown(self):
        """Close all browsers and stop Playwright."""
        while not self._available_browsers.empty():
            try:
                browser = self._available_browsers.get_nowait()
                if browser and browser.is_connected():
                    browser.close()
            except queue.Empty:
                break

        if self._playwright:
            self._playwright.stop()
            self._playwright = None
        self._created_browsers = 0


class ScrapingService:
    """Service for scraping websites and managing scraped content."""

    _FILE_EXTENSION_TYPES: Dict[str, str] = {
        "pdf": "PDF Document",
        "doc": "Word Document",
        "docx": "Word Document",
        "xls": "Excel Spreadsheet",
        "xlsx": "Excel Spreadsheet",
        "ppt": "PowerPoint Presentation",
        "pptx": "PowerPoint Presentation",
        "txt": "Text File",
        "csv": "CSV File",
        "zip": "ZIP Archive",
        "rar": "RAR Archive",
        "json": "JSON File",
        "xml": "XML File",
    }

    def __init__(self, client, mongo_db, vector_store_id: Optional[str] = None):
        """
        Initialize the scraping service.
        
        Args:
            client: OpenAI client for vector store uploads
            mongo_db: MongoDB database instance
            vector_store_id: OpenAI vector store ID for content storage
        """
        self.client = client
        self.db = mongo_db
        # NEW MODEL: Store content by URL, track which modes use it
        self.scraped_content_collection = mongo_db.get_collection("scraped_content")
        self.scraped_sites_collection = mongo_db.get_collection("scraped_sites")
        self.discovered_files_collection = mongo_db.get_collection("discovered_files")
        self.scrape_failures_collection = mongo_db.get_collection("scrape_failures")
        self.modes_collection = mongo_db.get_collection("modes")
        self.vector_store_id = vector_store_id
        self.local_dev_mode = config("LOCAL_DEV_MODE", default="false").lower() == "true"
        self.verification_scheduler = None  # Will be set by scheduler if needed
        self._browser_pool_size = int(config("SCRAPER_BROWSER_POOL_SIZE", default="2"))
        self._browser_pool = BrowserPool(
            self._start_playwright,
            max_browsers=self._browser_pool_size
        )
        atexit.register(self._browser_pool.shutdown)
        self._metrics_enabled = config("SCRAPER_ENABLE_METRICS", default="true").lower() == "true"
        self._sitemap_cache: Dict[str, Dict[str, Any]] = {}
        self._sitemap_cache_lock = threading.Lock()
        self._sitemap_cache_ttl = int(config("SCRAPER_SITEMAP_CACHE_SECONDS", default="3600"))
        self._requests_timeout = int(config("SCRAPER_REQUEST_TIMEOUT_SECONDS", default="15"))
        self._crawler_politeness_delay = float(config("SCRAPER_CRAWL_DELAY_SECONDS", default="0.3"))
        self._max_pdf_checks_per_page = int(config("SCRAPER_MAX_PDF_CHECKS_PER_PAGE", default="50"))
        self._embedded_pdf_checks_suppressed = False
        local_dev_mode = self.local_dev_mode
        embedded_pdf_default = "false" if local_dev_mode else "true"
        self._enable_embedded_pdf_checks = config(
            "SCRAPER_ENABLE_EMBEDDED_PDF_CHECKS",
            default=embedded_pdf_default
        ).lower() == "true"
        self._http_session = requests.Session()
        self._default_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; HTMXAssistantBot/1.0; +https://htmx-assistant)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        self._http_session.headers.update(self._default_headers)
        self._cached_psutil_process = psutil.Process(os.getpid()) if psutil else None
        self._metrics_lock = threading.Lock()
        self._active_job_metrics: Dict[str, Any] = {}
        
        # Create indexes for efficient lookups
        try:
            self.scraped_content_collection.create_index("normalized_url", unique=True)
            self.scraped_content_collection.create_index("base_domain")
            self.scraped_content_collection.create_index("verification_status")
            self.scraped_sites_collection.create_index("base_domain", unique=True)
            self.discovered_files_collection.create_index([("mode", 1), ("file_url", 1)], unique=True)
            self.discovered_files_collection.create_index("mode")
            self.scrape_failures_collection.create_index(
                [("normalized_url", 1), ("mode_name", 1)],
                unique=True,
            )
            self.scrape_failures_collection.create_index("base_domain")
            self.scrape_failures_collection.create_index("failed_at")
        except Exception:
            pass  # Indexes may already exist
    
    def _insert_discovered_file(self, file_info: Dict[str, Any]) -> bool:
        """Insert into discovered_files if (mode, file_url) is new."""
        file_url = file_info.get("file_url")
        mode_name = file_info.get("mode")
        if not file_url:
            return False
        query = {"file_url": file_url}
        if mode_name:
            query["mode"] = mode_name
        existing = self.discovered_files_collection.find_one(query, {"_id": 1})
        if existing:
            return False
        try:
            self.discovered_files_collection.insert_one(file_info)
            return True
        except Exception:
            return False
    
    def _detect_file_extension(self, url: Optional[str]) -> Optional[Tuple[str, str]]:
        """Return (extension, human-readable type) if URL points to a known downloadable file."""
        if not url:
            return None
        parsed = urlparse(url)
        path = (parsed.path or "").lower()
        for ext, file_type in self._FILE_EXTENSION_TYPES.items():
            if path.endswith(f".{ext}"):
                return ext, file_type
        return None

    def _build_file_metadata_from_url(
        self,
        file_url: str,
        *,
        link_text: Optional[str] = None,
        source_page_url: Optional[str] = None,
        source_page_title: Optional[str] = None,
        file_size: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        detected = self._detect_file_extension(file_url)
        if not detected:
            return None
        file_ext, file_type = detected
        parsed = urlparse(file_url)
        filename = os.path.basename(parsed.path) or f"document.{file_ext}"
        return {
            "file_url": file_url,
            "filename": filename,
            "file_type": file_type,
            "file_extension": file_ext,
            "link_text": link_text or filename,
            "file_size": file_size,
            "source_page_url": source_page_url or file_url,
            "source_page_title": source_page_title or (source_page_url or file_url),
        }

    def _record_direct_file_discovery(
        self,
        *,
        file_url: str,
        base_domain: Optional[str],
        mode_name: str,
        user_id: str,
        source_page_url: Optional[str] = None,
        source_page_title: Optional[str] = None,
        link_text: Optional[str] = None,
    ) -> bool:
        """Store a downloadable file without scraping the page."""
        metadata = self._build_file_metadata_from_url(
            file_url,
            link_text=link_text,
            source_page_url=source_page_url,
            source_page_title=source_page_title,
        )
        if not metadata:
            return False
        metadata["mode"] = mode_name
        metadata["discovered_at"] = datetime.utcnow()
        metadata["user_id"] = user_id
        metadata["status"] = "discovered"
        metadata["base_domain"] = base_domain
        inserted = self._insert_discovered_file(metadata)
        if inserted:
            print(f"  📄 Recorded direct file download: {file_url}")
        else:
            print(f"  ⏭️  Skipped duplicate direct file: {file_url}")
        return True
    
    def _record_failed_page(
        self,
        *,
        normalized_url: Optional[str],
        original_url: str,
        base_domain: Optional[str],
        mode_name: str,
        user_id: str,
        error: Optional[str],
        attempts: Optional[int],
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persist metadata about a failed scrape attempt."""
        collection = getattr(self, "scrape_failures_collection", None)
        if collection is None:
            return
        
        normalized = normalized_url or self._normalize_url(original_url)
        if not normalized:
            return
        
        now = datetime.utcnow()
        failure_context = dict(context or {})
        if attempts is not None:
            failure_context["attempts"] = attempts
        
        update_doc: Dict[str, Any] = {
            "$set": {
                "normalized_url": normalized,
                "original_url": original_url,
                "base_domain": base_domain,
                "mode_name": mode_name,
                "user_id": user_id,
                "last_error": error or "Unknown error",
                "failed_at": now,
                "context": failure_context,
            },
            "$setOnInsert": {
                "first_failed_at": now,
                "failure_count": 0,
            },
            "$inc": {"failure_count": 1},
        }
        
        try:
            collection.update_one(
                {"normalized_url": normalized, "mode_name": mode_name},
                update_doc,
                upsert=True,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"Warning: Could not record failed page {original_url}: {exc}")
    
    def _clear_failed_page(self, normalized_url: str, mode_name: str) -> None:
        """Remove failure metadata after a successful scrape."""
        collection = getattr(self, "scrape_failures_collection", None)
        if collection is None or not normalized_url:
            return
        
        try:
            collection.delete_one(
                {"normalized_url": normalized_url, "mode_name": mode_name}
            )
        except Exception as exc:  # noqa: BLE001
            print(f"Warning: Could not clear failed page record {normalized_url}: {exc}")
        
    def _get_memory_usage_mb(self) -> Optional[float]:
        if not psutil or not self._cached_psutil_process:
            return None
        try:
            return round(self._cached_psutil_process.memory_info().rss / (1024 * 1024), 2)
        except Exception:
            return None
    
    @contextmanager
    def _borrow_browser(self, browser=None):
        borrowed = None
        try:
            if browser:
                yield browser
            else:
                borrowed = self._browser_pool.acquire()
                yield borrowed
        finally:
            if borrowed:
                self._browser_pool.release(borrowed)
    
    def _http_get(self, url: str, **kwargs):
        timeout = kwargs.pop("timeout", self._requests_timeout)
        headers = kwargs.pop("headers", None)
        if headers:
            merged_headers = {
                **self._default_headers,
                **headers
            }
        else:
            merged_headers = self._default_headers
        return self._http_session.get(url, timeout=timeout, headers=merged_headers, **kwargs)
    
    def _init_site_metrics(self) -> Dict[str, Any]:
        return {
            "started_at": datetime.utcnow(),
            "duration_sec": 0.0,
            "memory_start_mb": self._get_memory_usage_mb(),
            "memory_end_mb": None,
            "memory_diff_mb": None,
            "pages": {
                "count": 0,
                "total_time_sec": 0.0,
                "max_time_sec": 0.0,
                "avg_time_sec": 0.0
            }
        }
    
    def _update_page_metrics(self, metrics: Dict[str, Any], duration: float):
        page_data = metrics["pages"]
        page_data["count"] += 1
        page_data["total_time_sec"] += duration
        if duration > page_data["max_time_sec"]:
            page_data["max_time_sec"] = duration
    
    def _finalize_site_metrics(self, metrics: Dict[str, Any], start_time: float):
        metrics["duration_sec"] = round(time.perf_counter() - start_time, 3)
        page_data = metrics["pages"]
        if page_data["count"]:
            page_data["avg_time_sec"] = round(
                page_data["total_time_sec"] / page_data["count"], 3
            )
        metrics["memory_end_mb"] = self._get_memory_usage_mb()
        if metrics["memory_start_mb"] is not None and metrics["memory_end_mb"] is not None:
            metrics["memory_diff_mb"] = round(
                metrics["memory_end_mb"] - metrics["memory_start_mb"], 2
            )
    
    def _stderr_supports_fileno(self) -> bool:
        """Check if sys.stderr exposes a working fileno (required by Playwright)."""
        stderr = sys.stderr
        if not hasattr(stderr, "fileno"):
            return False
        try:
            stderr.fileno()
            return True
        except (OSError, ValueError):
            return False
    
    @contextmanager
    def _playwright_stderr_fallback(self):
        """
        Provide a real file-backed stderr when running under environments (mod_wsgi)
        where the default log object lacks a file descriptor.
        """
        original_stderr = sys.stderr
        log_path = config(
            "PLAYWRIGHT_STDERR_FALLBACK_PATH",
            default=os.path.join(tempfile.gettempdir(), "playwright-stderr.log")
        )
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
        except Exception:
            # Directory may already exist or creation may fail (e.g., /tmp); ignore
            pass
        
        fallback_file = open(log_path, "a", buffering=1)
        sys.stderr = fallback_file
        try:
            yield
        finally:
            sys.stderr = original_stderr
            fallback_file.close()
    
    def _playwright_stderr_guard(self):
        """
        Return a context manager that ensures Playwright can access a stderr fileno.
        """
        if self._stderr_supports_fileno():
            return nullcontext()
        return self._playwright_stderr_fallback()
    
    def _start_playwright(self):
        """Start Playwright with stderr fallback handling."""
        with self._playwright_stderr_guard():
            return sync_playwright().start()
        
    def scrape_url(
        self, 
        url: str, 
        expand_accordions: bool = False,
        timeout: int = 30000,
        extract_files: bool = False,
        load_dynamic_content: bool = False,
        merge_dynamic_content: bool = False,
        playwright_browser=None,
        embedded_pdf_checks: Optional[bool] = None,
    ) -> Tuple[str, str, Optional[str], Optional[str], Optional[List[Dict]]]:
        """
        Scrape a single URL using Playwright.
        
        Args:
            url: URL to scrape
            expand_accordions: Whether to expand hidden/accordion content
            timeout: Page load timeout in milliseconds
            extract_files: Whether to extract downloadable file links
            load_dynamic_content: Whether to wait for and load dynamic content (slower)
            merge_dynamic_content: Whether to merge new dynamic content with initial content (for second pass)
            playwright_browser: Optional existing Playwright browser to reuse (for performance)
            
        Returns:
            Tuple of (content, title, error_message, html_content, file_links)
        """
        try:
            if not self._is_valid_url(url):
                return "", "", f"Invalid URL: {url}", None, None
            
            if embedded_pdf_checks is None:
                embedded_pdf_checks = self._enable_embedded_pdf_checks
            
            with self._borrow_browser(playwright_browser) as browser:
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                try:
                    wait_until = "networkidle" if load_dynamic_content else "domcontentloaded"
                    page.goto(url, timeout=timeout, wait_until=wait_until)
                    initial_title = page.title() or "Untitled"
                    pass_number = 1
                    
                    if merge_dynamic_content:
                        pass_number = 2
                        print(f"PASS {pass_number} - Starting hybrid scrape (static + dynamic merge)")
                        page.wait_for_timeout(500)
                        initial_html = page.content()
                        initial_content = self._extract_clean_text(initial_html, url)
                        initial_elements = self._get_text_elements(page)
                        print(f"  Initial content: {len(initial_content)} chars, {len(initial_elements)} elements")
                        self._wait_for_dynamic_content(page)
                        if expand_accordions:
                            self._expand_dynamic_elements_safe(page, url)
                        expanded_html = page.content()
                        expanded_content = self._extract_clean_text(expanded_html, url)
                        expanded_elements = self._get_text_elements(page)
                        print(f"  Expanded content: {len(expanded_content)} chars, {len(expanded_elements)} elements")
                        content = self._merge_content(initial_content, expanded_content, initial_elements, expanded_elements)
                        html_content = expanded_html
                        title = initial_title
                        print(f"  Final merged content: {len(content)} chars")
                        print(f"PASS {pass_number} TITLE: {title}")
                    elif load_dynamic_content:
                        pass_number = 2
                        self._wait_for_dynamic_content(page)
                        if expand_accordions:
                            self._expand_dynamic_elements(page)
                        html_content = page.content()
                        content = self._extract_clean_text(html_content, url)
                        title = initial_title
                        print(f"PASS {pass_number} TITLE: {title}")
                    else:
                        page.wait_for_timeout(500)
                        html_content = page.content()
                        content = self._extract_clean_text(html_content, url)
                        title = initial_title
                        print(f"PASS {pass_number} TITLE: {title}")
                    
                    print(f"PASS {pass_number} CONTENT LENGTH: {len(content)} chars")
                    file_links = None
                    if extract_files:
                        file_links = self._extract_file_links(
                            html_content,
                            url,
                            title,
                            playwright_browser=browser,
                            allow_embedded_checks=embedded_pdf_checks,
                        )
                    
                    if not content or len(content.strip()) < 100:
                        return "", title, "Insufficient content extracted (less than 100 characters)", html_content, file_links
                    
                    return content, title, None, html_content, file_links
                except PlaywrightTimeoutError:
                    return "", "", f"Timeout loading page: {url}", None, None
                except Exception as e:
                    return "", "", f"Error loading page: {str(e)}", None, None
                finally:
                    context.close()
        except Exception as e:
            return "", "", f"Browser error: {str(e)}", None, None
    
    def _wait_for_dynamic_content(self, page):
        """
        Wait for dynamically rendered content (SPAs, React, Vue, Angular, etc.)
        to fully load before scraping.
        
        First checks if page has JavaScript/dynamic content. If not, returns immediately.
        
        Args:
            page: Playwright page object
        """
        try:
            # Step 0: Check if page has JavaScript/dynamic content
            has_dynamic_content = page.evaluate("""
                () => {
                    // Check for script tags (excluding inline analytics/tracking)
                    const scripts = Array.from(document.querySelectorAll('script[src]'));
                    const hasAppScripts = scripts.length > 0;
                    
                    // Check for common JavaScript frameworks
                    const hasReact = !!(window.React || document.querySelector('[data-reactroot], [data-reactid]'));
                    const hasVue = !!(window.Vue || document.querySelector('[data-v-], [id^="app"].__vue__'));
                    const hasAngular = !!(window.angular || window.ng || document.querySelector('[ng-version], [ng-app]'));
                    const hasNext = !!window.__NEXT_DATA__;
                    const hasNuxt = !!window.__NUXT__;
                    
                    // Check for dynamic loading indicators
                    const hasLoadingElements = !!(
                        document.querySelector('.loading, .spinner, .loader, [class*="loading"], [class*="spinner"], [class*="skeleton"]')
                    );
                    
                    // Check for lazy loading attributes
                    const hasLazyLoad = !!(
                        document.querySelector('[loading="lazy"], [data-src], [data-lazy]')
                    );
                    
                    // Check if any fetch/AJAX calls are being made
                    const hasFetchAPI = !!window.fetch;
                    
                    // Consider it dynamic if any of these are true
                    return hasAppScripts || hasReact || hasVue || hasAngular || 
                           hasNext || hasNuxt || hasLoadingElements || 
                           hasLazyLoad || (hasFetchAPI && scripts.length > 2);
                }
            """)
            
            if not has_dynamic_content:
                print(f"  ⚡ Static page detected - skipping dynamic content wait")
                return  # Exit early for static pages
            
            # Dynamic content detected - proceed with full waiting logic
            print(f"  ⏳ Dynamic content detected - waiting for full render")
            
            # Step 1: Wait for common loading indicators to disappear
            loading_selectors = ".loading, .spinner, .loader, [class*='loading'], [class*='spinner'], [class*='skeleton'], #loading, [data-loading='true']"
            
            try:
                # Wait for loader to appear and then disappear (max 5 seconds)
                page.wait_for_selector(loading_selectors, timeout=3000, state="attached")
                page.wait_for_selector(loading_selectors, timeout=5000, state="detached")
            except Exception as e:
                print(f"SCRAPING: Error waiting for loading indicator: {e}")
            
            # Step 2: Wait for common JavaScript frameworks to finish rendering
            page.evaluate("""
                async () => {
                    // Wait for React
                    if (window.React || document.querySelector('[data-reactroot]')) {
                        await new Promise(resolve => {
                            if (typeof requestIdleCallback !== 'undefined') {
                                requestIdleCallback(resolve, { timeout: 2000 });
                            } else {
                                setTimeout(resolve, 1000);
                            }
                        });
                    }
                    
                    // Wait for Vue
                    if (window.Vue || document.querySelector('[data-v-]')) {
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                    
                    // Wait for Angular
                    if (window.angular || window.ng || document.querySelector('[ng-version]')) {
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                }
            """)
            
            # Step 3: Scroll to bottom to trigger lazy loading
            page.evaluate("""
                async () => {
                    const distance = 100;
                    const delay = 100;
                    let scrollHeight = document.body.scrollHeight;
                    
                    while (window.scrollY + window.innerHeight < scrollHeight) {
                        window.scrollBy(0, distance);
                        await new Promise(resolve => setTimeout(resolve, delay));
                        scrollHeight = document.body.scrollHeight;
                    }
                    
                    // Scroll back to top
                    window.scrollTo(0, 0);
                }
            """)
            
            # Step 4: Wait for DOM to stabilize (no new elements being added)
            page.evaluate("""
                () => {
                    return new Promise(resolve => {
                        let lastCount = document.querySelectorAll('*').length;
                        let stableCount = 0;
                        
                        const interval = setInterval(() => {
                            const currentCount = document.querySelectorAll('*').length;
                            
                            if (currentCount === lastCount) {
                                stableCount++;
                                if (stableCount >= 3) {  // Stable for 3 checks
                                    clearInterval(interval);
                                    resolve();
                                }
                            } else {
                                stableCount = 0;
                                lastCount = currentCount;
                            }
                        }, 500);
                        
                        // Timeout after 10 seconds
                        setTimeout(() => {
                            clearInterval(interval);
                            resolve();
                        }, 10000);
                    });
                }
            """)
            
            # Step 5: Final wait to ensure all rendering is complete
            page.wait_for_timeout(1000)
            
            # Step 6: Wait for images to load (optional, but helps with complete rendering)
            page.evaluate("""
                () => {
                    return Promise.all(
                        Array.from(document.images)
                            .filter(img => !img.complete)
                            .map(img => new Promise(resolve => {
                                img.onload = img.onerror = resolve;
                                // Timeout for slow images
                                setTimeout(resolve, 3000);
                            }))
                    );
                }
            """)
            
            print(f"  ✓ Dynamic content fully loaded")
            
        except Exception as e:
            print(f"  ⚠ Error waiting for dynamic content: {e}")
            # Fall back to simple wait
            page.wait_for_timeout(3000)
    
    def _expand_dynamic_elements(self, page):
        """
        Find and expand accordion sections, tabs, and other hidden content.
        
        Args:
            page: Playwright page object
        """
        try:
            # Common accordion/expandable selectors
            expandable_selectors = [
                "button[aria-expanded='false']",
                ".accordion-button:not(.collapsed)",
                "[class*='accordion']",
                "[class*='collaps']",
                "[class*='expand']",
                "details:not([open])",
                "[role='tab']",
                "[data-toggle='collapse']",
            ]
            
            for selector in expandable_selectors:
                try:
                    elements = page.query_selector_all(selector)
                    for element in elements:  # Expand ALL elements, no limit
                        try:
                            if element.is_visible():
                                print(f"SCRAPING: Expanding {element.text_content()}")
                                element.click(timeout=1000)
                                page.wait_for_timeout(500)  # Wait for animation
                        except Exception:
                            continue
                except Exception:
                    continue
            
            # Open all <details> elements
            page.evaluate("""
                () => {
                    document.querySelectorAll('details:not([open])').forEach(el => {
                        el.setAttribute('open', '');
                    });
                }
            """)
            
        except Exception as e:
            print(f"Error expanding dynamic elements: {e}")
    
    def _expand_dynamic_elements_safe(self, page, original_url: str):
        """
        Safely expand accordion sections and dynamic content without navigating away.
        
        Args:
            page: Playwright page object
            original_url: Original URL to verify we don't navigate away
        """
        try:
            # Common accordion/expandable selectors
            expandable_selectors = [
                "button[aria-expanded='false']",
                ".accordion-button:not(.collapsed)",
                "[class*='accordion']",
                "[class*='collaps']",
                "[class*='expand']",
                "details:not([open])",
                "[role='tab']",
                "[data-toggle='collapse']",
            ]
            
            for selector in expandable_selectors:
                try:
                    elements = page.query_selector_all(selector)
                    print(f"SCRAPING: Safely expanding {len(elements)} elements matching '{selector}'")
                    for element in elements:  # Expand ALL elements, no limit
                        try:
                            if element.is_visible():
                                # Check if element has href that would navigate away
                                href = element.get_attribute('href')
                                if href and not href.startswith('#') and href != 'javascript:void(0)' and href != 'javascript:;':
                                    # Skip elements that would navigate to different pages
                                    continue
                                
                                # Get current URL before click
                                current_url = page.url
                                
                                # Click and wait briefly
                                element.click(timeout=1000)
                                page.wait_for_timeout(500)
                                
                                # Verify we're still on the same page
                                if page.url != current_url:
                                    print(f"  ⚠ Navigation detected, going back to {current_url}")
                                    page.goto(current_url, wait_until="domcontentloaded", timeout=10000)
                                    page.wait_for_timeout(500)
                                    break  # Stop expanding for this selector
                        except Exception:
                            continue
                except Exception:
                    continue
            
            # Open all <details> elements (these are safe and don't navigate)
            page.evaluate("""
                () => {
                    document.querySelectorAll('details:not([open])').forEach(el => {
                        el.setAttribute('open', '');
                    });
                }
            """)
            
            print(f"  ✓ Safely expanded dynamic elements")
            
        except Exception as e:
            print(f"  ⚠ Error safely expanding dynamic elements: {e}")
    
    def _get_text_elements(self, page) -> Dict[str, str]:
        """
        Extract text elements with their identifiers for comparison.
        
        Args:
            page: Playwright page object
            
        Returns:
            Dictionary mapping element identifiers to their text content
        """
        try:
            elements = page.evaluate("""
                () => {
                    const elements = {};
                    let counter = 0;
                    
                    // Get all text-containing elements
                    const textElements = document.querySelectorAll('p, div, span, h1, h2, h3, h4, h5, h6, li, td, th, article, section');
                    
                    textElements.forEach((el) => {
                        // Only include elements with direct text content
                        const text = el.innerText?.trim();
                        if (text && text.length > 10) {  // Minimum 10 chars
                            // Create a unique identifier based on tag, classes, and position
                            const id = el.id || '';
                            const classes = el.className || '';
                            const tag = el.tagName.toLowerCase();
                            const key = `${tag}_${id}_${classes}_${counter}`;
                            elements[key] = text.substring(0, 500);  // Limit to 500 chars per element
                            counter++;
                        }
                    });
                    
                    return elements;
                }
            """)
            return elements
        except Exception as e:
            print(f"  ⚠ Error getting text elements: {e}")
            return {}
    
    def _merge_content(
        self, 
        initial_content: str, 
        expanded_content: str,
        initial_elements: Dict[str, str],
        expanded_elements: Dict[str, str]
    ) -> str:
        """
        Merge expanded content with initial content, adding only new text that appeared.
        Ignores duplicate text values and removes repeated paragraphs.
        
        Args:
            initial_content: Initial scraped content
            expanded_content: Content after expansion
            initial_elements: Text elements before expansion
            expanded_elements: Text elements after expansion
            
        Returns:
            Merged content with new dynamic content appended and duplicates removed
        """
        try:
            # Find new text that wasn't in the initial elements
            new_texts = []
            seen_texts = set()  # Track texts we've already added to avoid duplicates
            
            # First, add all initial element texts to seen set (normalized)
            for initial_text in initial_elements.values():
                seen_texts.add(self._normalize_text_for_comparison(initial_text))
            
            # Also add normalized chunks from initial content
            initial_chunks = self._split_into_chunks(initial_content)
            for chunk in initial_chunks:
                seen_texts.add(self._normalize_text_for_comparison(chunk))
            
            for key, text in expanded_elements.items():
                normalized_text = self._normalize_text_for_comparison(text)
                
                # Skip if we've already seen this exact text
                if normalized_text in seen_texts:
                    continue
                
                # Check if this text existed in initial elements
                found_in_initial = False
                for initial_key, initial_text in initial_elements.items():
                    # Check for substantial overlap (more than 80% similarity)
                    if text in initial_text or initial_text in text:
                        found_in_initial = True
                        break
                    # Check for high word overlap
                    initial_words = set(initial_text.lower().split())
                    expanded_words = set(text.lower().split())
                    if initial_words and expanded_words:
                        overlap = len(initial_words.intersection(expanded_words))
                        similarity = overlap / max(len(initial_words), len(expanded_words))
                        if similarity > 0.8:
                            found_in_initial = True
                            break
                
                # This is new content that appeared after expansion
                if not found_in_initial and text not in initial_content:
                    # Check against other new texts to avoid duplicates in the new content itself
                    is_duplicate_in_new = False
                    for existing_new_text in new_texts:
                        if self._texts_are_similar(text, existing_new_text, threshold=0.85):
                            is_duplicate_in_new = True
                            break
                    
                    if not is_duplicate_in_new:
                        new_texts.append(text)
                        seen_texts.add(normalized_text)
            
            # If we found new content, append it to the initial content
            if new_texts:
                print(f"  📝 Found {len(new_texts)} unique new text sections after expansion")
                merged = initial_content + "\n\n--- Additional Content (Expanded) ---\n\n"
                merged += "\n\n".join(new_texts)
                
                # Final deduplication pass on the entire merged content
                merged = self._deduplicate_content(merged)
                return merged
            else:
                print(f"  ℹ No new content found after expansion")
                # Still deduplicate the initial content in case it has duplicates
                return self._deduplicate_content(initial_content)
                
        except Exception as e:
            print(f"  ⚠ Error merging content: {e}")
            # Fallback to expanded content if merge fails, but still deduplicate
            return self._deduplicate_content(expanded_content)
    
    def _normalize_text_for_comparison(self, text: str) -> str:
        """
        Normalize text for comparison by removing extra whitespace and lowercasing.
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text string
        """
        # Remove extra whitespace, newlines, tabs
        normalized = ' '.join(text.split())
        # Lowercase for case-insensitive comparison
        normalized = normalized.lower()
        # Remove common punctuation variations that don't affect meaning
        normalized = normalized.replace('\xa0', ' ')  # Non-breaking space
        return normalized.strip()
    
    def _split_into_chunks(self, content: str) -> List[str]:
        """
        Split content into chunks (paragraphs/sections) for duplicate detection.
        
        Args:
            content: Content to split
            
        Returns:
            List of content chunks
        """
        # Split by double newlines (paragraphs) or lines
        chunks = []
        
        # First try splitting by double newlines (paragraphs)
        paragraphs = content.split('\n\n')
        for para in paragraphs:
            para = para.strip()
            if len(para) > 20:  # Minimum 20 chars to be considered a chunk
                chunks.append(para)
        
        # If no paragraphs found, split by single newlines
        if not chunks:
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if len(line) > 20:
                    chunks.append(line)
        
        return chunks
    
    def _texts_are_similar(self, text1: str, text2: str, threshold: float = 0.85) -> bool:
        """
        Check if two texts are similar based on word overlap.
        
        Args:
            text1: First text
            text2: Second text
            threshold: Similarity threshold (0.0 to 1.0)
            
        Returns:
            True if texts are similar, False otherwise
        """
        # Normalize both texts
        norm1 = self._normalize_text_for_comparison(text1)
        norm2 = self._normalize_text_for_comparison(text2)
        
        # Exact match after normalization
        if norm1 == norm2:
            return True
        
        # Check substring containment
        if norm1 in norm2 or norm2 in norm1:
            return True
        
        # Calculate word overlap similarity
        words1 = set(norm1.split())
        words2 = set(norm2.split())
        
        if not words1 or not words2:
            return False
        
        overlap = len(words1.intersection(words2))
        similarity = overlap / max(len(words1), len(words2))
        
        return similarity >= threshold
    
    def _deduplicate_content(self, content: str) -> str:
        """
        Remove duplicate paragraphs and repeated lines from content.
        Preserves the first occurrence and removes subsequent duplicates.
        
        Args:
            content: Content to deduplicate
            
        Returns:
            Deduplicated content string
        """
        try:
            lines = content.split('\n')
            
            # Preserve the Source: line at the beginning
            source_line = None
            start_idx = 0
            if lines and lines[0].startswith('Source:'):
                source_line = lines[0]
                start_idx = 1
                # Skip empty lines after source
                while start_idx < len(lines) and not lines[start_idx].strip():
                    start_idx += 1
            
            # Process remaining content
            content_lines = lines[start_idx:]
            
            # Track seen content (normalized for comparison)
            seen_normalized = set()
            seen_paragraphs = set()
            deduplicated_lines = []
            
            # Build paragraphs from lines
            current_paragraph = []
            
            for line in content_lines:
                stripped = line.strip()
                
                # Empty line marks paragraph boundary
                if not stripped:
                    if current_paragraph:
                        # Check if this paragraph is a duplicate
                        para_text = '\n'.join(current_paragraph)
                        para_normalized = self._normalize_text_for_comparison(para_text)
                        
                        # Check for exact duplicate or similar paragraph
                        is_duplicate = False
                        if para_normalized in seen_paragraphs:
                            is_duplicate = True
                        else:
                            # Check similarity against all seen paragraphs
                            for seen_para in seen_paragraphs:
                                # Use length-aware comparison to avoid false positives
                                if len(para_normalized) > 30 and len(seen_para) > 30:
                                    words1 = set(para_normalized.split())
                                    words2 = set(seen_para.split())
                                    if words1 and words2:
                                        overlap = len(words1.intersection(words2))
                                        similarity = overlap / max(len(words1), len(words2))
                                        if similarity > 0.90:  # High threshold for paragraph deduplication
                                            is_duplicate = True
                                            break
                        
                        if not is_duplicate:
                            # Add this paragraph
                            deduplicated_lines.extend(current_paragraph)
                            deduplicated_lines.append('')  # Empty line
                            seen_paragraphs.add(para_normalized)
                        
                        current_paragraph = []
                    else:
                        # Preserve empty lines between paragraphs (but not multiple)
                        if deduplicated_lines and deduplicated_lines[-1] != '':
                            deduplicated_lines.append('')
                else:
                    # Check for duplicate single lines (for lists, short items, etc.)
                    line_normalized = self._normalize_text_for_comparison(stripped)
                    
                    # Skip very short lines from duplicate checking (< 15 chars)
                    if len(line_normalized) < 15:
                        current_paragraph.append(line)
                    else:
                        # Check if this line is a duplicate
                        if line_normalized not in seen_normalized:
                            current_paragraph.append(line)
                            seen_normalized.add(line_normalized)
            
            # Handle last paragraph if exists
            if current_paragraph:
                para_text = '\n'.join(current_paragraph)
                para_normalized = self._normalize_text_for_comparison(para_text)
                
                is_duplicate = False
                if para_normalized in seen_paragraphs:
                    is_duplicate = True
                else:
                    for seen_para in seen_paragraphs:
                        if len(para_normalized) > 30 and len(seen_para) > 30:
                            words1 = set(para_normalized.split())
                            words2 = set(seen_para.split())
                            if words1 and words2:
                                overlap = len(words1.intersection(words2))
                                similarity = overlap / max(len(words1), len(words2))
                                if similarity > 0.90:
                                    is_duplicate = True
                                    break
                
                if not is_duplicate:
                    deduplicated_lines.extend(current_paragraph)
                    seen_paragraphs.add(para_normalized)
            
            # Rebuild content
            result_lines = []
            if source_line:
                result_lines.append(source_line)
                result_lines.append('')  # Empty line after source
            
            result_lines.extend(deduplicated_lines)
            
            # Remove trailing empty lines
            while result_lines and not result_lines[-1].strip():
                result_lines.pop()
            
            deduplicated = '\n'.join(result_lines)
            
            # Calculate statistics
            original_line_count = len(lines)
            deduplicated_line_count = len(result_lines)
            removed_count = original_line_count - deduplicated_line_count
            
            if removed_count > 0:
                print(f"  🧹 Removed {removed_count} duplicate lines/paragraphs")
            
            return deduplicated
            
        except Exception as e:
            print(f"  ⚠ Error deduplicating content: {e}")
            return content  # Return original content if deduplication fails
    
    def _extract_clean_text(self, html_content: str, url: str) -> str:
        """
        Extract clean text from HTML content.
        
        Args:
            html_content: Raw HTML content
            url: Source URL for reference
            
        Returns:
            Clean text content
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script, style, and other non-content elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe']):
            element.decompose()
        
        # Get text content
        text = soup.get_text(separator='\n', strip=True)
        
        # Clean up whitespace
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line]  # Remove empty lines
        
        # Remove duplicate consecutive lines
        cleaned_lines = []
        prev_line = None
        for line in lines:
            if line != prev_line:
                cleaned_lines.append(line)
            prev_line = line
        
        content = '\n'.join(cleaned_lines)
        
        # Add source URL at the beginning
        content = f"Source: {url}\n\n{content}"
        
        return content
    
    def _is_valid_url(self, url: str) -> bool:
        """
        Validate URL format and protocol.
        
        Args:
            url: URL to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            result = urlparse(url)
            return all([result.scheme in ['http', 'https'], result.netloc])
        except Exception:
            return False
    
    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL for consistent comparison and deduplication.
        Removes fragments, sorts query parameters, normalizes path.
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL string
        """
        try:
            parsed = urlparse(url)
            
            # Remove www. prefix for consistency
            netloc = parsed.netloc.lower()
            if netloc.startswith('www.'):
                netloc = netloc[4:]
            
            # Remove trailing slash from path (unless it's the root)
            path = parsed.path.rstrip('/') if parsed.path != '/' else '/'
            
            # Remove fragment
            # Keep query string as-is (could sort params, but may break some sites)
            
            normalized = urlunparse((
                parsed.scheme.lower(),
                netloc,
                path,
                parsed.params,
                parsed.query,
                ''  # Remove fragment
            ))
            
            return normalized
        except Exception:
            return url
    
    def _get_base_domain(self, url: str) -> str:
        """
        Extract base domain from URL for comparison.
        
        Args:
            url: URL to extract domain from
            
        Returns:
            Base domain (e.g., 'example.com')
        """
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    
    def _is_single_page_url(self, url: str) -> bool:
        """
        Determine if a URL is a specific page or a base domain.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL points to a specific page, False if it's a base domain
        """
        try:
            parsed = urlparse(url)
            path = parsed.path.rstrip('/')
            
            # If there's a path beyond just '/', it's a single page
            # Examples: /forms, /about/contact, /index.html
            if path and path != '':
                return True
            
            # If there's a query string, it's a specific page
            if parsed.query:
                return True
            
            # Otherwise it's a base domain
            return False
        except Exception:
            return False
    
    def _is_same_domain(self, url1: str, url2: str) -> bool:
        """
        Check if two URLs are from the same domain.
        
        Args:
            url1: First URL
            url2: Second URL
            
        Returns:
            True if same domain
        """
        return self._get_base_domain(url1) == self._get_base_domain(url2)
    
    def _discover_sitemap(self, base_url: str) -> List[str]:
        """
        Try to discover and parse sitemap.xml from a website.
        
        Args:
            base_url: Base URL of the website
            
        Returns:
            List of URLs found in sitemap
        """
        base_domain = self._get_base_domain(base_url)
        now = time.time()
        with self._sitemap_cache_lock:
            cache_entry = self._sitemap_cache.get(base_domain)
            if cache_entry and now - cache_entry["timestamp"] < self._sitemap_cache_ttl:
                return cache_entry["urls"]
        
        urls = []
        parsed = urlparse(base_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        
        # Common sitemap locations
        sitemap_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap-index.xml',
            '/sitemap1.xml',
            '/sitemap',
            '/robots.txt',  # Check robots.txt for sitemap reference
        ]
        
        for path in sitemap_paths:
            sitemap_url = base + path
            try:
                if path == '/robots.txt':
                    # Parse robots.txt for sitemap reference
                    response = self._http_get(sitemap_url)
                    if response.status_code == 200:
                        for line in response.text.split('\n'):
                            if line.lower().startswith('sitemap:'):
                                actual_sitemap_url = line.split(':', 1)[1].strip()
                                urls.extend(self._parse_sitemap(actual_sitemap_url))
                else:
                    # Try to parse as XML sitemap
                    urls.extend(self._parse_sitemap(sitemap_url))
                
                if urls:  # If we found URLs, stop searching
                    print(f"Found sitemap at {sitemap_url} with {len(urls)} URLs")
                    break
                    
            except Exception as e:
                continue
        
        with self._sitemap_cache_lock:
            self._sitemap_cache[base_domain] = {
                "timestamp": now,
                "urls": urls
            }
        
        return urls
    
    def _parse_sitemap(self, sitemap_url: str) -> List[str]:
        """
        Parse a sitemap XML file and extract URLs.
        Converts relative URLs to absolute URLs based on sitemap location.
        
        Args:
            sitemap_url: URL of the sitemap
            
        Returns:
            List of absolute URLs found
        """
        urls = []
        try:
            response = self._http_get(sitemap_url)
            if response.status_code != 200:
                return urls
            
            # Parse XML
            root = ET.fromstring(response.content)
            
            # Handle namespace
            namespaces = {
                'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'xhtml': 'http://www.w3.org/1999/xhtml'
            }
            
            # Check if this is a sitemap index (contains other sitemaps)
            sitemaps = root.findall('.//sm:sitemap/sm:loc', namespaces)
            if sitemaps:
                # This is a sitemap index, recursively parse each sitemap
                print(f"Found sitemap index with {len(sitemaps)} sitemaps")
                for sitemap in sitemaps:  # Parse ALL sitemaps, no limit
                    # Convert relative sitemap URLs to absolute
                    sitemap_loc = sitemap.text.strip() if sitemap.text else ""
                    if sitemap_loc:
                        absolute_sitemap_url = urljoin(sitemap_url, sitemap_loc)
                        urls.extend(self._parse_sitemap(absolute_sitemap_url))
            else:
                # Regular sitemap with URLs
                url_elements = root.findall('.//sm:url/sm:loc', namespaces)
                for url_elem in url_elements:
                    if url_elem.text:
                        url_text = url_elem.text.strip()
                        # Convert relative URLs to absolute URLs
                        absolute_url = urljoin(sitemap_url, url_text)
                        urls.append(absolute_url)
            
        except Exception as e:
            print(f"Error parsing sitemap {sitemap_url}: {e}")
        
        return urls
    
    def _extract_links_from_html(self, html_content: str, base_url: str) -> Set[str]:
        """
        Extract all links from HTML content.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            
        Returns:
            Set of absolute URLs found on the page
        """
        links = set()
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all <a> tags with href
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                
                # Skip empty, javascript, mailto, tel, and anchor links
                if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    continue
                
                # Convert relative URLs to absolute
                absolute_url = urljoin(base_url, href)
                
                # Only keep HTTP(S) URLs from the same domain
                if self._is_valid_url(absolute_url) and self._is_same_domain(absolute_url, base_url):
                    normalized = self._normalize_url(absolute_url)
                    links.add(normalized)
            
        except Exception as e:
            print(f"Error extracting links: {e}")
        
        return links
    
    def _is_pdf_viewer_page(self, url: str) -> bool:
        """
        Check if a URL likely points to a PDF viewer page rather than a direct PDF.
        
        Args:
            url: URL to check
            
        Returns:
            True if likely a PDF viewer page
        """
        viewer_indicators = [
            'viewer', 'view', 'preview', 'display', 'show',
            'document', 'file', 'attachment', 'download', 'media', 'pdf'
        ]
        
        url_lower = url.lower()
        
        # Check if URL contains viewer indicators but NOT a .pdf extension
        has_viewer_indicator = any(indicator in url_lower for indicator in viewer_indicators)
        has_pdf_in_url = '.pdf' in url_lower or 'pdf' in url_lower
        is_direct_pdf = url_lower.endswith('.pdf')
        
        # Likely a viewer if it has indicators and PDF reference but doesn't end in .pdf
        return has_viewer_indicator and has_pdf_in_url and not is_direct_pdf
    
    def _extract_pdf_from_viewer(self, viewer_url: str, link_text: str = "", playwright_browser=None) -> Optional[Dict]:
        """
        Visit a PDF viewer page and extract the actual PDF download URL.
        
        Args:
            viewer_url: URL of the PDF viewer page
            link_text: Text of the original link
            playwright_browser: Optional existing Playwright browser to reuse
            
        Returns:
            Dict with PDF file metadata if found, None otherwise
        """
        try:
            with self._borrow_browser(playwright_browser) as browser:
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                try:
                    page.goto(viewer_url, timeout=15000, wait_until="domcontentloaded")
                    page.wait_for_timeout(1000)  # Brief wait for viewer to load
                    
                    # Look for download/save buttons and extract PDF URL
                    pdf_url = page.evaluate("""
                        () => {
                            // Common selectors for download buttons
                            const downloadSelectors = [
                                'a[download]',
                                'button[title*="download" i]',
                                'button[aria-label*="download" i]',
                                'a[title*="download" i]',
                                'a[href*=".pdf"]',
                                'button:has-text("Download")',
                                'button:has-text("Save")',
                                'a:has-text("Download PDF")',
                                '[class*="download"]',
                                '[id*="download"]',
                                'iframe[src*=".pdf"]',
                                'embed[src*=".pdf"]',
                                'object[data*=".pdf"]'
                            ];
                            
                            // Try each selector
                            for (const selector of downloadSelectors) {
                                try {
                                    const elements = document.querySelectorAll(selector);
                                    for (const el of elements) {
                                        // Check href attribute
                                        const href = el.getAttribute('href');
                                        if (href && (href.endsWith('.pdf') || href.includes('.pdf?'))) {
                                            return href;
                                        }
                                        
                                        // Check data attributes
                                        const dataUrl = el.getAttribute('data-url') || 
                                                      el.getAttribute('data-src') || 
                                                      el.getAttribute('data-file');
                                        if (dataUrl && (dataUrl.endsWith('.pdf') || dataUrl.includes('.pdf?'))) {
                                            return dataUrl;
                                        }
                                        
                                        // Check src for iframes/embeds
                                        const src = el.getAttribute('src') || el.getAttribute('data');
                                        if (src && (src.endsWith('.pdf') || src.includes('.pdf?'))) {
                                            return src;
                                        }
                                    }
                                } catch (e) {
                                    continue;
                                }
                            }
                            
                            // Look for PDF URLs in onclick handlers or data attributes
                            const allElements = document.querySelectorAll('*');
                            for (const el of allElements) {
                                const onclick = el.getAttribute('onclick');
                                if (onclick && onclick.includes('.pdf')) {
                                    const match = onclick.match(/['"]([^'"]*\\.pdf[^'"]*)['"]/);
                                    if (match) return match[1];
                                }
                            }
                            
                            return null;
                        }
                    """)
                finally:
                    context.close()
            
            if not pdf_url:
                return None
            
            absolute_pdf_url = urljoin(viewer_url, pdf_url)
            parsed = urlparse(absolute_pdf_url)
            filename = os.path.basename(parsed.path)
            if not filename or not filename.endswith('.pdf'):
                filename = "document.pdf"
            
            print(f"  📄 Extracted PDF from viewer: {filename}")
            
            return {
                'file_url': absolute_pdf_url,
                'filename': filename,
                'file_type': 'PDF Document',
                'file_extension': 'pdf',
                'link_text': link_text or filename,
                'file_size': None,
                'viewer_url': viewer_url,
                'extracted_from_viewer': True
            }
        except Exception as e:
            print(f"  ⚠ Error extracting PDF from viewer: {e}")
            return None
    
    def _check_for_embedded_pdf_sync(self, page_url: str, link_text: str = "", playwright_browser=None) -> Optional[Dict]:
        """
        Internal synchronous method to check for embedded PDFs.
        Runs in a separate thread to avoid asyncio conflicts.
        
        Args:
            page_url: URL of the page to check
            link_text: Text of the original link
            playwright_browser: Optional existing Playwright browser to reuse (usually None for thread safety)
            
        Returns:
            Dict with PDF file metadata if found, None otherwise
        """
        try:
            # Note: In practice, playwright_browser will usually be None here since this runs in a thread
            # But we support it for potential future use cases
            if playwright_browser:
                browser = playwright_browser
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                should_close_browser = False
            else:
                p = self._start_playwright()
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                should_close_browser = True
            
            # Track PDF requests intercepted
            intercepted_pdf_url = None
            intercepted_pdf_urls = []  # Track all potential PDF URLs
            download_triggered = False  # Track if a download was triggered
            download_filename = None  # Original filename from download
            
            def handle_response(response):
                nonlocal intercepted_pdf_url
                try:
                    url = response.url
                    
                    # Check Content-Type header
                    content_type = response.headers.get('content-type', '').lower()
                    
                    # Check if this is a PDF response
                    if 'application/pdf' in content_type or url.endswith('.pdf') or '.pdf?' in url:
                        if not intercepted_pdf_url:  # Use the first PDF found
                            intercepted_pdf_url = url
                        intercepted_pdf_urls.append(url)
                except Exception as e:
                    pass  # Ignore errors in response handler
            
            def handle_download(download):
                nonlocal download_triggered, intercepted_pdf_url, download_filename
                print("download triggered")
                try:
                    download_url = download.url
                    if not intercepted_pdf_url:
                        intercepted_pdf_url = download_url
                    download_triggered = True
                    
                    # Capture the suggested filename
                    suggested_filename = download.suggested_filename
                    download_filename = suggested_filename
                    
                    # Cancel the download - we just need the URL
                    download.cancel()
                    
                except Exception as e:
                    # Still capture URL even if something fails
                    pass
            
            # Listen to responses to check content-type headers
            page.on("response", handle_response)
            # Listen for download events (triggered when PDF is served directly)
            page.on("download", handle_download)
            
            try:
                # Navigate to the page
                try:
                    page.goto(page_url, timeout=15000, wait_until="domcontentloaded")
                except Exception as nav_error:
                    # Check if this was due to a download trigger
                    if download_triggered or "Download" in str(nav_error):
                        # The download handler already captured the URL
                        pass
                    else:
                        # Re-raise if it's a different error
                        raise
                
                # Wait for any dynamic content or downloads to trigger
                if not download_triggered:
                    page.wait_for_timeout(2000)
                
                # Check for embed elements with PDF content (skip if download was triggered)
                embed_info = None
                if not download_triggered:
                    try:
                        embed_info = page.evaluate("""
                            () => {
                                // Look for embed elements with PDF type
                                const embeds = document.querySelectorAll('embed[type="application/pdf"], object[type="application/pdf"], iframe[type="application/pdf"]');
                                
                                console.log('Found', embeds.length, 'embed/object/iframe elements with PDF type');
                                
                                for (const embed of embeds) {
                                    const src = embed.getAttribute('src') || embed.getAttribute('data');
                                    const name = embed.getAttribute('name');
                                    const id = embed.getAttribute('id');
                                    
                                    console.log('PDF embed found:', {
                                        tag: embed.tagName,
                                        src: src,
                                        name: name,
                                        id: id,
                                        type: embed.getAttribute('type')
                                    });
                                    
                                    if (src) {
                                        return {
                                            found: true,
                                            src: src,
                                            type: embed.tagName.toLowerCase(),
                                            name: name,
                                            id: id
                                        };
                                    }
                                }
                                
                                // Also check for embed/object/iframe with PDF in src
                                const allEmbeds = document.querySelectorAll('embed, object, iframe');
                                console.log('Checking', allEmbeds.length, 'total embed/object/iframe elements');
                                
                                for (const embed of allEmbeds) {
                                    const src = embed.getAttribute('src') || embed.getAttribute('data');
                                    if (src && (src.endsWith('.pdf') || src.includes('.pdf?') || src.includes('pdf'))) {
                                        console.log('PDF element found by src pattern:', {
                                            tag: embed.tagName,
                                            src: src
                                        });
                                        
                                        return {
                                            found: true,
                                            src: src,
                                            type: embed.tagName.toLowerCase()
                                        };
                                    }
                                }
                                
                                console.log('No PDF embeds found on page');
                                return { found: false };
                            }
                        """)
                    except Exception as eval_error:
                        embed_info = {'found': False}
                else:
                    embed_info = {'found': False}
                
                # Priority 1: Use intercepted PDF URL from network requests
                result = None
                
                if intercepted_pdf_url:
                    # Extract filename from URL
                    parsed = urlparse(intercepted_pdf_url)
                    filename = os.path.basename(parsed.path)
                    if not filename or not filename.endswith('.pdf'):
                        filename = "embedded_document.pdf"
                    
                    result = {
                        'file_url': intercepted_pdf_url,
                        'filename': download_filename or filename,
                        'file_type': 'PDF Document (Embedded)',
                        'file_extension': 'pdf',
                        'link_text': link_text or filename,
                        'file_size': None,
                        'embed_page_url': page_url,
                        'is_embedded': True,
                        'embed_type': 'intercepted',
                        'detected_via': 'network_interception'
                    }
                
                # Priority 2: Check if page itself is a PDF (direct serve)
                if not result:
                    # If download was triggered, treat it as direct PDF serve
                    if download_triggered:
                        print("download triggered in priority 2")
                        # Use the page URL as the PDF URL
                        parsed = urlparse(page_url)
                        filename = os.path.basename(parsed.path)
                        if not filename or not filename.endswith('.pdf'):
                            filename = "document.pdf"
                        
                        result = {
                            'file_url': page_url,
                            'filename': download_filename or filename,
                            'file_type': 'PDF Document (Embedded)',
                            'file_extension': 'pdf',
                            'link_text': link_text or filename,
                            'file_size': None,
                            'embed_page_url': page_url,
                            'is_embedded': True,
                            'embed_type': 'direct',
                            'detected_via': 'download_trigger'
                        }
                    else:
                        try:
                            content_type = page.evaluate("""
                                () => {
                                    return document.contentType || document.mimeType || '';
                                }
                            """)
                            
                            if content_type and 'pdf' in content_type.lower():
                                # The page itself is serving a PDF
                                parsed = urlparse(page_url)
                                filename = os.path.basename(parsed.path)
                                if not filename or not filename.endswith('.pdf'):
                                    filename = "document.pdf"
                                
                                result = {
                                    'file_url': page_url,
                                    'filename': download_filename or filename,
                                    'file_type': 'PDF Document (Embedded)',
                                    'file_extension': 'pdf',
                                    'link_text': link_text or filename,
                                    'file_size': None,
                                    'embed_page_url': page_url,
                                    'is_embedded': True,
                                    'embed_type': 'direct',
                                    'detected_via': 'direct_serve'
                                }
                        except Exception as e:
                            pass
                
                # Priority 3: Check embed elements with valid src
                if not result and embed_info and embed_info.get('found'):
                    pdf_src = embed_info.get('src', '')
                    
                    # If src is a valid URL, use it
                    if pdf_src and pdf_src not in ['about:blank', 'about:srcdoc', '']:
                        # Make URL absolute if relative
                        absolute_pdf_url = urljoin(page_url, pdf_src)
                        
                        # Verify it's a valid PDF URL
                        if absolute_pdf_url.endswith('.pdf') or '.pdf?' in absolute_pdf_url:
                            # Extract filename from URL
                            parsed = urlparse(absolute_pdf_url)
                            filename = os.path.basename(parsed.path)
                            if not filename or not filename.endswith('.pdf'):
                                filename = "embedded_document.pdf"
                            
                            result = {
                                'file_url': absolute_pdf_url,
                                'filename': download_filename or filename,
                                'file_type': 'PDF Document (Embedded)',
                                'file_extension': 'pdf',
                                'link_text': link_text or filename,
                                'file_size': None,
                                'embed_page_url': page_url,
                                'is_embedded': True,
                                'embed_type': embed_info.get('type', 'embed'),
                                'detected_via': 'embed_src'
                            }
                
                # Final result summary - only log if found
                if result:
                    print(f"  ✅ Found embedded PDF: {result.get('filename')}")
                
                # Close context but keep browser if reusing
                context.close()
                if should_close_browser:
                    browser.close()
                    if 'p' in locals():
                        p.stop()
                
                return result
                    
            except Exception as e:
                context.close()
                if should_close_browser:
                    browser.close()
                    if 'p' in locals():
                        p.stop()
                print(f"  ⚠ Error checking for embedded PDF: {e}")
                return None
                    
        except Exception as e:
            print(f"  ⚠ Browser error while checking for embedded PDF: {e}")
            return None
    
    def _check_for_embedded_pdf(self, page_url: str, link_text: str = "", playwright_browser=None) -> Optional[Dict]:
        """
        Check if a page contains an embedded PDF and extract it.
        Runs the sync Playwright code in a thread pool to avoid asyncio conflicts.
        
        Args:
            page_url: URL of the page to check
            link_text: Text of the original link
            playwright_browser: Ignored for thread safety (each thread creates its own browser)
            
        Returns:
            Dict with PDF file metadata if found, None otherwise
        """
        import concurrent.futures
        
        if not self._enable_embedded_pdf_checks or self._embedded_pdf_checks_suppressed:
            return None
        
        try:
            # Note: We don't pass playwright_browser to the thread for thread safety
            # Each embedded PDF check gets its own browser instance
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._check_for_embedded_pdf_sync, page_url, link_text, None)
                result = future.result(timeout=20)  # 20 second timeout
                return result
        except concurrent.futures.TimeoutError:
            print(f"  ⚠ Timeout checking for embedded PDF: {page_url}")
            return None
        except RuntimeError as e:
            message = str(e).lower()
            if "cannot schedule new futures after interpreter shutdown" in message:
                print("  ⚠ Embedded PDF checks cancelled during interpreter shutdown; suppressing further checks")
                self._embedded_pdf_checks_suppressed = True
                return None
            print(f"  ⚠ Error in embedded PDF check: {e}")
            return None
        except Exception as e:
            print(f"  ⚠ Error in embedded PDF check: {e}")
            return None
    
    def _extract_file_links(
        self,
        html_content: str,
        base_url: str,
        source_page_title: str,
        playwright_browser=None,
        allow_embedded_checks: Optional[bool] = None,
    ) -> List[Dict]:
        """
        Extract downloadable file links from HTML content.
        Also checks for PDF viewer pages and extracts actual PDF URLs.
        Also checks for embedded PDFs on linked pages.
        Deduplicates files by URL to prevent duplicates.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            source_page_title: Title of the source page
            
        Returns:
            List of dicts with file metadata (url, filename, type, etc.)
        """
        files = []
        pdf_viewer_urls = []  # Track potential PDF viewer URLs to check
        embed_check_urls = []  # Track URLs to check for embedded PDFs
        seen_file_urls = set()  # Track seen file URLs to prevent duplicates
        remaining_pdf_checks = self._max_pdf_checks_per_page
        if allow_embedded_checks is None:
            allow_embedded_checks = self._enable_embedded_pdf_checks
        effective_embedded_checks = (
            allow_embedded_checks and not self._embedded_pdf_checks_suppressed
        )

        print(f"Extracting file links from {source_page_title} on {base_url}")
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all <a> tags with href
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                
                # Skip empty links
                if not href:
                    continue
                
                # Convert relative URLs to absolute
                absolute_url = urljoin(base_url, href)
                
                # Get link text for context
                link_text = link.get_text(strip=True)
                
                file_metadata = self._build_file_metadata_from_url(
                    absolute_url,
                    link_text=link_text,
                    source_page_url=base_url,
                    source_page_title=source_page_title,
                )
                
                if file_metadata:
                    normalized_file_url = self._normalize_url(file_metadata["file_url"])
                    if normalized_file_url in seen_file_urls:
                        continue
                    
                    size_match = re.search(r'[\(\[]?\s*(\d+\.?\d*\s*(?:KB|MB|GB))\s*[\)\]]?', link_text, re.IGNORECASE)
                    if size_match:
                        file_metadata["file_size"] = size_match.group(1)
                    
                    files.append(file_metadata)
                    seen_file_urls.add(normalized_file_url)
                    
                elif self._is_pdf_viewer_page(absolute_url):
                    # Potential PDF viewer page - add to list for checking
                    pdf_viewer_urls.append((absolute_url, link_text))
                elif effective_embedded_checks:
                    # Check if this link might lead to a page with an embedded PDF
                    # Only check links from the same domain to avoid excessive crawling
                    if self._is_same_domain(absolute_url, base_url):
                        embed_check_urls.append((absolute_url, link_text))
                else:
                    continue

            print(f"Found {len(pdf_viewer_urls)} PDF viewer URLs")
            print(f"Found {len(embed_check_urls)} embed check URLs")
            print(f"Found {len(files)} files")
            # Process PDF viewer URLs (check all for thoroughness)
            if pdf_viewer_urls:
                duplicates_skipped = 0
                
                for viewer_url, link_text in pdf_viewer_urls:  # Check ALL viewers, no limit
                    if remaining_pdf_checks <= 0:
                        break
                    pdf_info = self._extract_pdf_from_viewer(viewer_url, link_text, playwright_browser=playwright_browser)
                    
                    if pdf_info:
                        # Normalize the extracted PDF URL for duplicate checking
                        normalized_pdf_url = self._normalize_url(pdf_info['file_url'])
                        
                        # Skip if we've already seen this PDF URL
                        if normalized_pdf_url in seen_file_urls:
                            duplicates_skipped += 1
                            continue
                        
                        # Add source page info
                        pdf_info['source_page_url'] = base_url
                        pdf_info['source_page_title'] = source_page_title
                        files.append(pdf_info)
                        
                        # Mark this PDF URL as seen
                        seen_file_urls.add(normalized_pdf_url)
                        
                        # Small delay to be polite
                        time.sleep(0.5)
                        remaining_pdf_checks -= 1
                
                if duplicates_skipped > 0:
                    print(f"  ⏭️  Skipped {duplicates_skipped} duplicate PDF(s) from viewers")
            
            if not effective_embedded_checks:
                if embed_check_urls:
                    if not allow_embedded_checks:
                        print("Embedded PDF checks disabled for this scrape")
                    elif self._embedded_pdf_checks_suppressed:
                        print("Embedded PDF checks suppressed after previous errors")
                    else:
                        print("Embedded PDF checks disabled via SCRAPER_ENABLE_EMBEDDED_PDF_CHECKS")
            else:
                print(f"Processing {len(embed_check_urls)} embed check URLs")
            # Process URLs that might have embedded PDFs (check all links for thoroughness)
            if (
                effective_embedded_checks
                and embed_check_urls
                and remaining_pdf_checks > 0
            ):
                duplicates_skipped = 0
                
                for embed_url, link_text in embed_check_urls:  # Check ALL links, no limit
                    if remaining_pdf_checks <= 0:
                        break
                    embedded_pdf = self._check_for_embedded_pdf(embed_url, link_text, playwright_browser=playwright_browser)
                    
                    if embedded_pdf:
                        # Normalize the embedded PDF URL for duplicate checking
                        normalized_pdf_url = self._normalize_url(embedded_pdf['file_url'])
                        
                        # Skip if we've already seen this PDF URL
                        if normalized_pdf_url in seen_file_urls:
                            duplicates_skipped += 1
                            continue
                        
                        # Add source page info
                        embedded_pdf['source_page_url'] = base_url
                        embedded_pdf['source_page_title'] = source_page_title
                        files.append(embedded_pdf)
                        
                        # Mark this PDF URL as seen
                        seen_file_urls.add(normalized_pdf_url)
                        
                        # Small delay to be polite
                        time.sleep(0.5)
                        remaining_pdf_checks -= 1
                
                if duplicates_skipped > 0:
                    print(f"  ⏭️  Skipped {duplicates_skipped} duplicate embedded PDF(s)")
            
        except Exception as e:
            print(f"Error extracting file links: {e}")
        
        print(f"Returning {len(files)} files")
        return files
    
    def _crawl_site(
        self,
        start_url: str,
        max_pages: int = 3000,
        max_depth: int = 3
    ) -> Set[str]:
        """
        Crawl a website starting from a URL, discovering all pages.
        Uses sitemap if available, otherwise follows links.
        Ensures no duplicate URLs are added.
        
        Args:
            start_url: Starting URL (can be homepage or any page)
            max_pages: Maximum number of pages to discover
            max_depth: Maximum crawl depth for link following
            
        Returns:
            Set of normalized URLs to scrape (no duplicates)
        """
        urls_to_scrape = set()
        
        # Normalize the start URL
        start_url = self._normalize_url(start_url)
        base_domain = self._get_base_domain(start_url)
        
        print(f"Starting crawl of {base_domain}")
        
        # Step 1: Try to find sitemap
        sitemap_urls = self._discover_sitemap(start_url)
        if sitemap_urls:
            print(f"Found {len(sitemap_urls)} URLs in sitemap")
            initial_count = len(urls_to_scrape)
            skipped_different_domain = 0
            skipped_duplicates = 0
            
            for url in sitemap_urls:
                if self._is_same_domain(url, start_url):
                    normalized = self._normalize_url(url)
                    # Explicit duplicate check (set handles this automatically, but be explicit)
                    if normalized not in urls_to_scrape:
                        urls_to_scrape.add(normalized)
                    else:
                        skipped_duplicates += 1
                    if len(urls_to_scrape) >= max_pages:
                        break
                else:
                    skipped_different_domain += 1
            
            added_count = len(urls_to_scrape) - initial_count
            print(f"  Added {added_count} URLs from sitemap")
            if skipped_duplicates > 0:
                print(f"  Skipped {skipped_duplicates} duplicate URLs")
            if skipped_different_domain > 0:
                print(f"  Skipped {skipped_different_domain} URLs from different domains")
                # Debug: Show an example of filtered URL
                for url in sitemap_urls[:3]:
                    if not self._is_same_domain(url, start_url):
                        print(f"    Example filtered URL: {url}")
                        print(f"    Start URL domain: {self._get_base_domain(start_url)}")
                        print(f"    Filtered URL domain: {self._get_base_domain(url)}")
                        break
        
        # Step 2: If we don't have enough URLs, crawl by following links
        if len(urls_to_scrape) < max_pages:
            print(f"Crawling by following links (current: {len(urls_to_scrape)} URLs)")
            
            visited = set()
            queued = set()  # Track URLs already in the to_visit queue
            # Ensure start_url is normalized before adding to queue
            normalized_start = self._normalize_url(start_url)
            to_visit = [(normalized_start, 0)]  # (url, depth)
            queued.add(normalized_start)
            pages_crawled = 0
            
            while to_visit and len(urls_to_scrape) < max_pages:
                current_url, depth = to_visit.pop(0)
                queued.discard(current_url)  # Remove from queued set as we process it
                
                # Skip if already visited or depth exceeded
                if current_url in visited or depth > max_depth:
                    continue
                
                visited.add(current_url)
                urls_to_scrape.add(current_url)
                pages_crawled += 1
                
                # Progress logging every 10 pages
                if pages_crawled % 10 == 0:
                    print(f"  Crawling progress: {pages_crawled} pages crawled, {len(urls_to_scrape)} total URLs, {len(visited)} visited, {len(to_visit)} in queue")
                
                # If we haven't reached max depth, extract links
                if depth < max_depth:
                    try:
                        # Use requests for faster link extraction (no need for Playwright here)
                        response = self._http_get(current_url)
                        if response.status_code == 200:
                            links = self._extract_links_from_html(response.text, current_url)
                            
                            new_links = 0
                            for link in links:
                                # Normalize the link to ensure consistency
                                # (links are already normalized by _extract_links_from_html, but we do it again for safety)
                                normalized_link = self._normalize_url(link)
                                
                                # Comprehensive duplicate checking:
                                # 1. Not already visited
                                # 2. Not already queued for visiting
                                # 3. Not already in the final urls_to_scrape set
                                if (normalized_link not in visited and 
                                    normalized_link not in queued and 
                                    normalized_link not in urls_to_scrape):
                                    to_visit.append((normalized_link, depth + 1))
                                    queued.add(normalized_link)
                                    new_links += 1
                            
                            # Log if we found new links
                            if new_links > 0 and pages_crawled <= 5:  # Only log details for first 5 pages
                                print(f"    Found {new_links} new links on {current_url}")
                        
                        # Small delay to be polite
                        time.sleep(0.5)
                        
                    except Exception as e:
                        print(f"  Error crawling {current_url}: {e}")
                        continue
            
            print(f"  Crawling complete: {pages_crawled} pages crawled")
        
        print(f"Discovered {len(urls_to_scrape)} unique URLs for {base_domain}")
        return urls_to_scrape
    
    def upload_to_vector_store(
        self, 
        content: str, 
        mode: str, 
        url: str,
        title: str,
        scraped_at: datetime
    ) -> Optional[str]:
        """
        Upload scraped content to OpenAI vector store.
        
        Args:
            content: Scraped text content
            mode: Mode name
            url: Source URL
            title: Page title
            scraped_at: Scraping timestamp
            
        Returns:
            OpenAI file ID or None if upload fails
        """
        if not self.vector_store_id:
            print("No vector store ID configured")
            return None
        
        try:
            # Create markdown content with metadata
            markdown_content = f"""# {title}

**Source URL:** {url}
**Scraped:** {scraped_at.strftime('%Y-%m-%d %H:%M:%S UTC')}
**Mode:** {mode}

---

{content}
"""
            
            # Generate filename from URL
            filename = self._generate_filename(url, mode)
            
            if self.local_dev_mode:
                # TEMPORARY: Save file locally before uploading
                try:
                    local_dir = "scraped_files"
                    os.makedirs(local_dir, exist_ok=True)
                    local_path = os.path.join(local_dir, filename)
                    with open(local_path, 'w', encoding='utf-8') as f:
                        f.write(markdown_content)
                    print(f"Saved scraped content locally to: {local_path}")
                except Exception as e:
                    print(f"Warning: Could not save file locally: {e}")
            
            # Create a file-like object
            file_stream = io.BytesIO(markdown_content.encode('utf-8'))
            
            # Upload to OpenAI
            uploaded_file = self.client.files.create(
                file=(filename, file_stream),
                purpose="assistants"
            )
            
            # Add to vector store with metadata
            self.client.vector_stores.files.create(
                vector_store_id=self.vector_store_id,
                file_id=uploaded_file.id,
                attributes={
                    "mode": mode,
                    "source": "scraped_site",
                    "url": url,
                    "scraped_at": scraped_at.isoformat()
                }
            )
            
            return uploaded_file.id
            
        except Exception as e:
            print(f"Error uploading to vector store: {e}")
            return None
    
    def _generate_filename(self, url: str, mode: str) -> str:
        """
        Generate a safe filename from URL.
        
        Args:
            url: Source URL
            mode: Mode name
            
        Returns:
            Safe filename
        """
        # Extract domain and path
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '')
        path = parsed.path.strip('/').replace('/', '_')
        
        # Create safe filename
        if path:
            filename = f"{mode}_{domain}_{path[:50]}.md"
        else:
            filename = f"{mode}_{domain}.md"
        
        # Remove invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        return filename
    
    def scrape_mode_sites(
        self, 
        mode_name: str, 
        user_id: str,
        max_retries: int = 2,
        max_pages_per_site: int = 5000,
        max_crawl_depth: int = 3,
        progress_callback=None,
        resume_state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, any]:
        """
        Scrape all configured sites for a mode with full site crawling.
        Automatically discovers sitemaps and follows links.
        Reuses existing scraped content across modes.
        
        Args:
            mode_name: Name of the mode
            user_id: User ID (owner of the mode)
            max_retries: Maximum retry attempts for failed scrapes
            max_pages_per_site: Maximum pages to discover per site
            max_crawl_depth: Maximum depth for link following
            progress_callback: Optional callback function(progress_dict) called during scraping
            resume_state: Optional checkpoint data with pending sites/URLs to resume work
            
        Returns:
            Dictionary with scraping results
        """
        # Get mode configuration
        mode_doc = self.modes_collection.find_one({"name": mode_name})
        if not mode_doc:
            return {"error": "Mode not found", "success": False}
        
        scrape_sites = mode_doc.get("scrape_sites", [])
        if not scrape_sites:
            return {"error": "No sites configured for scraping", "success": False}
        
        sanitized_sites = [site.strip() for site in scrape_sites if site and site.strip()]
        if not sanitized_sites:
            return {"error": "No valid sites configured for scraping", "success": False}
        
        resume_payload = resume_state if isinstance(resume_state, dict) else {}
        pending_from_resume = [
            site.strip() for site in resume_payload.get("pending_sites", []) if site and site.strip()
        ] if resume_payload else []
        pending_sites_queue = deque(pending_from_resume or sanitized_sites)
        if not pending_sites_queue:
            return {"error": "No pending sites to process", "success": False}
        
        checkpoint_state: Dict[str, Any] = {}
        processed_domains: Set[str] = set()
        
        def _update_checkpoint(current_site=None, remaining_urls=None, total_urls=None, processed_urls=None, pending_override=None):
            pending_snapshot = []
            if pending_override is not None:
                pending_snapshot = [site for site in pending_override if site]
            else:
                if current_site:
                    pending_snapshot.append(current_site)
                pending_snapshot.extend([site for site in list(pending_sites_queue) if site])
            
            state = {
                "pending_sites": pending_snapshot,
                "current_site": current_site,
                "updated_at": datetime.utcnow().isoformat()
            }
            if remaining_urls is not None:
                state["current_site_remaining_urls"] = list(remaining_urls)
            else:
                state.pop("current_site_remaining_urls", None)
            if total_urls is not None:
                state["current_site_total_urls"] = total_urls
            else:
                state.pop("current_site_total_urls", None)
            if processed_urls is not None:
                state["current_site_processed_urls"] = processed_urls
            else:
                state.pop("current_site_processed_urls", None)
            
            checkpoint_state.clear()
            checkpoint_state.update(state)
            return state
        
        def _checkpoint_payload():
            if not checkpoint_state:
                return None
            payload = {
                "pending_sites": list(checkpoint_state.get("pending_sites", [])),
                "current_site": checkpoint_state.get("current_site"),
                "updated_at": checkpoint_state.get("updated_at")
            }
            if "current_site_remaining_urls" in checkpoint_state:
                payload["current_site_remaining_urls"] = list(checkpoint_state.get("current_site_remaining_urls") or [])
            if "current_site_total_urls" in checkpoint_state:
                payload["current_site_total_urls"] = checkpoint_state.get("current_site_total_urls")
            if "current_site_processed_urls" in checkpoint_state:
                payload["current_site_processed_urls"] = checkpoint_state.get("current_site_processed_urls")
            return payload
        
        def _emit_progress(progress_payload: Optional[Dict[str, Any]] = None):
            if not progress_callback:
                return
            payload = progress_payload.copy() if progress_payload else {}
            checkpoint_payload = _checkpoint_payload()
            if checkpoint_payload:
                payload["checkpoint"] = checkpoint_payload
            progress_callback(payload)
        
        _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
        
        newly_scraped_ids: List[str] = []
        
        results = {
            "success": True,
            "mode": mode_name,
            "total_sites": len(sanitized_sites),
            "total_pages_scraped": 0,
            "total_pages_reused": 0,
            "total_pages_failed": 0,
            "sites": []
        }
        
        resume_consumed = False
        
        while pending_sites_queue:
            site_url = pending_sites_queue.popleft()
            if not site_url:
                continue
            
            site_resume_state = None
            if not resume_consumed and resume_payload and resume_payload.get("current_site") == site_url:
                site_resume_state = resume_payload
                resume_consumed = True
            
            base_domain = self._get_base_domain(site_url)
            if base_domain:
                processed_domains.add(base_domain)
            is_single_page = self._is_single_page_url(site_url)
            site_metrics = self._init_site_metrics() if self._metrics_enabled else None
            site_timer = time.perf_counter() if self._metrics_enabled else None
            
            def _record_page(duration: float = 0.0):
                if site_metrics:
                    self._update_page_metrics(site_metrics, duration)
            
            print(f"\n{'='*60}")
            if is_single_page:
                print(f"Processing single page: {site_url}")
            else:
                print(f"Processing site: {base_domain} for mode '{mode_name}'")
            print(f"{'='*60}")
            
            site_result = {
                "base_url": site_url,
                "domain": base_domain,
                "pages_scraped": 0,
                "pages_reused": 0,
                "pages_failed": 0,
                "status": "in_progress",
                "is_single_page": is_single_page
            }

            def _scrape_url_batch_for_site(
                url_list: List[str],
                *,
                total_urls: Optional[int] = None,
                pages_processed_start: int = 0,
                label: str = "site"
            ) -> Dict[str, int]:
                """
                Scrape a list of URLs for the current site and update shared metrics.
                Returns a dict with scraped, failed, reused, and processed counts.
                """
                urls = [url for url in url_list if url]
                if not urls:
                    return {
                        "scraped": 0,
                        "failed": 0,
                        "reused": 0,
                        "processed": pages_processed_start
                    }
                total_urls_value = total_urls or len(urls)
                page_queue = deque(urls)
                print(f"Creating browser instance for scraping {len(page_queue)} {label} page(s)...")
                p = self._start_playwright()
                browser = p.chromium.launch(headless=True)
                scraped_count = 0
                failed_count = 0
                reused_count = 0
                pages_processed = pages_processed_start
                processed_urls = set()
                try:
                    while page_queue:
                        url = page_queue.popleft()
                        pages_processed += 1
                        idx = pages_processed
                        normalized_url = self._normalize_url(url)
                        if not normalized_url:
                            print(f"  ⚠️  Skipping invalid URL: {url}")
                            continue
                        if normalized_url in processed_urls:
                            print(f"\nSkipping duplicate URL (already processed): {url}")
                            continue
                        processed_urls.add(normalized_url)
                        print(f"\nScraping {label} page {idx}/{total_urls_value}: {url}")
                        if self._record_direct_file_discovery(
                            file_url=url,
                            base_domain=base_domain,
                            mode_name=mode_name,
                            user_id=user_id,
                            source_page_url=url,
                            source_page_title=f"{base_domain or url} (direct file)",
                        ):
                            _update_checkpoint(
                                current_site=site_url,
                                remaining_urls=list(page_queue),
                                total_urls=total_urls_value,
                                processed_urls=pages_processed
                            )
                            _emit_progress({
                                "current_site": base_domain,
                                "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                                "scraped_pages": results["total_pages_scraped"],
                                "reused_pages": results["total_pages_reused"],
                                "failed_pages": results["total_pages_failed"]
                            })
                            continue
                        existing = self.scraped_content_collection.find_one(
                            {"normalized_url": normalized_url}
                        )
                        if existing:
                            self.scraped_content_collection.update_one(
                                {"_id": existing["_id"]},
                                {"$addToSet": {"modes": mode_name}}
                            )
                            self._clear_failed_page(normalized_url, mode_name)
                            site_result["pages_reused"] += 1
                            results["total_pages_reused"] += 1
                            reused_count += 1
                            _update_checkpoint(
                                current_site=site_url,
                                remaining_urls=list(page_queue),
                                total_urls=total_urls_value,
                                processed_urls=pages_processed
                            )
                            _emit_progress({
                                "current_site": base_domain,
                                "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                                "scraped_pages": results["total_pages_scraped"],
                                "reused_pages": results["total_pages_reused"],
                                "failed_pages": results["total_pages_failed"]
                            })
                            continue
                        retry_count = 0
                        success = False
                        error = None
                        page_start = None
                        while retry_count <= max_retries and not success:
                            page_start = time.perf_counter()
                            content, title, error, html_content, file_links = self.scrape_url(
                                url,
                                extract_files=False,
                                load_dynamic_content=False,
                                playwright_browser=browser
                            )
                            if error:
                                retry_count += 1
                                if retry_count <= max_retries:
                                    print(f"  ↻ Retry {retry_count}/{max_retries}")
                                    time.sleep(2 ** retry_count)
                                continue
                            scraped_at = datetime.utcnow()
                            openai_file_id = self.upload_to_vector_store(
                                content, mode_name, url, title, scraped_at
                            )
                            content_doc = {
                                "normalized_url": normalized_url,
                                "original_url": url,
                                "base_domain": base_domain,
                                "title": title,
                                "content": content,
                                "scraped_at": scraped_at,
                                "openai_file_id": openai_file_id,
                                "status": "active",
                                "verification_status": "pending_verification",
                                "modes": [mode_name],
                                "user_id": user_id,
                                "metadata": {
                                    "word_count": len(content.split()),
                                    "char_count": len(content)
                                }
                            }
                            insert_result = self.scraped_content_collection.insert_one(content_doc)
                            newly_scraped_ids.append(str(insert_result.inserted_id))
                            self._clear_failed_page(normalized_url, mode_name)
                            if file_links:
                                for file_info in file_links:
                                    file_info["mode"] = mode_name
                                    file_info["discovered_at"] = scraped_at
                                    file_info["user_id"] = user_id
                                    file_info["status"] = "discovered"
                                    file_info["base_domain"] = base_domain
                                    self._insert_discovered_file(file_info)
                                print(f"  📄 Discovered {len(file_links)} downloadable file(s)")
                            scraped_count += 1
                            site_result["pages_scraped"] += 1
                            results["total_pages_scraped"] += 1
                            success = True
                            print(f"  ✓ Successfully scraped ({content_doc['metadata']['word_count']} words)")
                            _record_page(time.perf_counter() - page_start)
                            time.sleep(1)
                        if not success:
                            failed_count += 1
                            site_result["pages_failed"] += 1
                            results["total_pages_failed"] += 1
                            print(f"  ✗ Failed: {error}")
                            self._record_failed_page(
                                normalized_url=normalized_url,
                                original_url=url,
                                base_domain=base_domain,
                                mode_name=mode_name,
                                user_id=user_id,
                                error=error,
                                attempts=retry_count,
                                context={
                                    "site_url": site_url,
                                    "page_index": idx,
                                    "total_urls": total_urls_value,
                                    "max_retries": max_retries,
                                    "label": label
                                },
                            )
                            if site_metrics and page_start:
                                _record_page(time.perf_counter() - page_start)
                        _update_checkpoint(
                            current_site=site_url,
                            remaining_urls=list(page_queue),
                            total_urls=total_urls_value,
                            processed_urls=pages_processed
                        )
                        _emit_progress({
                            "current_site": base_domain,
                            "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                            "scraped_pages": results["total_pages_scraped"],
                            "reused_pages": results["total_pages_reused"],
                            "failed_pages": results["total_pages_failed"]
                        })
                finally:
                    print(f"Closing browser instance...")
                    browser.close()
                    p.stop()
                return {
                    "scraped": scraped_count,
                    "failed": failed_count,
                    "reused": reused_count,
                    "processed": pages_processed
            }
            
            # If it's a single page, scrape only that page
            if is_single_page:
                print(f"Scraping single page: {site_url}")
                
                normalized_url = self._normalize_url(site_url)
                single_remaining = [site_url]
                total_single_urls = 1
                processed_single = 0
                
                if site_resume_state and site_resume_state.get("current_site_remaining_urls") is not None:
                    saved_remaining = [
                        url for url in site_resume_state.get("current_site_remaining_urls", []) if url
                    ]
                    if saved_remaining:
                        single_remaining = saved_remaining
                    else:
                        single_remaining = []
                    total_single_urls = site_resume_state.get("current_site_total_urls") or max(1, len(single_remaining) or 1)
                    processed_single = site_resume_state.get("current_site_processed_urls") or max(0, total_single_urls - len(single_remaining))
                
                if not single_remaining:
                    _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                    _emit_progress({})
                    print("  ✓ Single page already processed, skipping")
                    site_result["status"] = "completed"
                    results["sites"].append(site_result)
                    continue
                
                _update_checkpoint(
                    current_site=site_url,
                    remaining_urls=single_remaining,
                    total_urls=total_single_urls,
                    processed_urls=processed_single
                )
                _emit_progress({})
                
                if self._record_direct_file_discovery(
                    file_url=site_url,
                    base_domain=base_domain,
                    mode_name=mode_name,
                    user_id=user_id,
                    source_page_url=site_url,
                    source_page_title=f"{base_domain or site_url} (direct file)",
                ):
                    site_result["status"] = "completed"
                    _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                    _emit_progress({})
                    results["sites"].append(site_result)
                    continue

                # Check if this specific page was already scraped
                existing = self.scraped_content_collection.find_one({
                    "normalized_url": normalized_url
                })
                
                if existing:
                    # Page already exists, just link to mode
                    self.scraped_content_collection.update_one(
                        {"_id": existing["_id"]},
                        {"$addToSet": {"modes": mode_name}}
                    )
                    self._clear_failed_page(normalized_url, mode_name)
                    site_result["pages_reused"] = 1
                    results["total_pages_reused"] += 1
                    site_result["status"] = "reused"
                    _record_page()
                    print(f"  ✓ Reused existing content")
                    
                    _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                    
                    # Update progress
                    _emit_progress({
                        "current_site": site_url,
                        "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                        "scraped_pages": results["total_pages_scraped"],
                        "reused_pages": results["total_pages_reused"],
                        "failed_pages": results["total_pages_failed"]
                    })
                else:
                    # Scrape the single page
                    retry_count = 0
                    success = False
                    error = None
                    
                    while retry_count <= max_retries and not success:
                        page_start = time.perf_counter()
                        # First pass: Fast scrape without dynamic content (no file discovery)
                        content, title, error, html_content, file_links = self.scrape_url(
                            site_url, 
                            extract_files=False,  # First pass: content only, no file discovery
                            load_dynamic_content=False,
                            embedded_pdf_checks=self._enable_embedded_pdf_checks,
                        )
                        
                        if error:
                            retry_count += 1
                            if retry_count <= max_retries:
                                print(f"  ↻ Retry {retry_count}/{max_retries}")
                                time.sleep(2 ** retry_count)
                            continue
                        
                        # Upload to vector store
                        scraped_at = datetime.utcnow()
                        openai_file_id = self.upload_to_vector_store(
                            content, mode_name, site_url, title, scraped_at
                        )
                        
                        # Save to MongoDB with verification status
                        content_doc = {
                            "normalized_url": normalized_url,
                            "original_url": site_url,
                            "base_domain": base_domain,
                            "title": title,
                            "content": content,
                            "scraped_at": scraped_at,
                            "openai_file_id": openai_file_id,
                            "status": "active",
                            "verification_status": "pending_verification",  # Track verification status
                            "modes": [mode_name],
                            "user_id": user_id,
                            "metadata": {
                                "word_count": len(content.split()),
                                "char_count": len(content),
                                "is_single_page": True
                            }
                        }
                        
                        insert_result = self.scraped_content_collection.insert_one(content_doc)
                        newly_scraped_ids.append(str(insert_result.inserted_id))
                        self._clear_failed_page(normalized_url, mode_name)
                        
                        # Store discovered files
                        if file_links:
                            for file_info in file_links:
                                file_info["mode"] = mode_name
                                file_info["discovered_at"] = scraped_at
                                file_info["user_id"] = user_id
                                file_info["status"] = "discovered"
                                file_info["base_domain"] = base_domain
                                self._insert_discovered_file(file_info)
                            
                            print(f"  📄 Discovered {len(file_links)} downloadable file(s)")
                        
                        site_result["pages_scraped"] = 1
                        results["total_pages_scraped"] += 1
                        site_result["status"] = "completed"
                        success = True
                        _record_page(time.perf_counter() - page_start)
                        print(f"  ✓ Successfully scraped ({content_doc['metadata']['word_count']} words)")
                        
                        _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                        
                        # Update progress
                        _emit_progress({
                            "current_site": site_url,
                            "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                            "scraped_pages": results["total_pages_scraped"],
                            "reused_pages": results["total_pages_reused"],
                            "failed_pages": results["total_pages_failed"]
                        })
                    
                    if not success:
                        site_result["pages_failed"] = 1
                        results["total_pages_failed"] += 1
                        site_result["status"] = "failed"
                        print(f"  ✗ Failed: {error}")
                        self._record_failed_page(
                            normalized_url=normalized_url,
                            original_url=site_url,
                            base_domain=base_domain,
                            mode_name=mode_name,
                            user_id=user_id,
                            error=error,
                            attempts=retry_count,
                            context={
                                "is_single_page": True,
                                "max_retries": max_retries,
                                "site_url": site_url,
                            },
                        )
                        
                        _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                        
                        # Update progress
                        _emit_progress({
                            "current_site": site_url,
                            "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                            "scraped_pages": results["total_pages_scraped"],
                            "reused_pages": results["total_pages_reused"],
                            "failed_pages": results["total_pages_failed"]
                        })
                
                results["sites"].append(site_result)
                continue  # Skip the full site crawl logic below
            
            # Full site crawling logic (for base domain URLs)
            # Step 1: Check if this site has been scraped before
            site_doc = self.scraped_sites_collection.find_one({"base_domain": base_domain})
            
            if site_doc:
                # Site was already scraped - just link it to this mode
                print(f"Site {base_domain} already scraped. Linking to mode '{mode_name}'...")
                
                # Link all pages from this site to the mode
                existing_pages = self.scraped_content_collection.find({
                    "base_domain": base_domain,
                    "status": "active"
                })
                
                reused_count = 0
                for page in existing_pages:
                    # Add this mode to the page's modes list
                    self.scraped_content_collection.update_one(
                        {"_id": page["_id"]},
                        {"$addToSet": {"modes": mode_name}}
                    )
                    normalized_existing = page.get("normalized_url")
                    if normalized_existing:
                        self._clear_failed_page(normalized_existing, mode_name)
                    reused_count += 1
                    _record_page()
                
                site_result["pages_reused"] = reused_count
                site_result["status"] = "reused"
                results["total_pages_reused"] += reused_count
                
                print(f"Reused {reused_count} existing pages from {base_domain}")
                _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                
                # Update progress
                _emit_progress({
                    "current_site": base_domain,
                    "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                    "scraped_pages": results["total_pages_scraped"],
                    "reused_pages": results["total_pages_reused"],
                    "failed_pages": results["total_pages_failed"]
                })

                # Check sitemap coverage to ensure no pages are missing
                sitemap_urls = self._discover_sitemap(site_url)
                sitemap_backfill_scraped = 0
                sitemap_backfill_failed = 0
                if sitemap_urls:
                    normalized_map: Dict[str, str] = {}
                    for raw_url in sitemap_urls:
                        if not raw_url:
                            continue
                        if not self._is_same_domain(raw_url, site_url):
                            continue
                        normalized = self._normalize_url(raw_url)
                        if normalized:
                            normalized_map[normalized] = raw_url
                    sitemap_total = len(normalized_map)
                    if sitemap_total:
                        existing_cursor = self.scraped_content_collection.find(
                            {"base_domain": base_domain, "status": "active"},
                            {"normalized_url": 1}
                        )
                        existing_urls = {
                            doc.get("normalized_url")
                            for doc in existing_cursor
                            if doc.get("normalized_url")
                        }
                        current_scraped_count = len(existing_urls)
                        print(
                            f"Sitemap for {base_domain} reports {sitemap_total} URL(s); "
                            f"{current_scraped_count} page(s) currently scraped."
                        )
                        missing_normalized = [
                            normalized for normalized in normalized_map.keys()
                            if normalized and normalized not in existing_urls
                        ]
                        allowed_missing = None
                        if max_pages_per_site:
                            allowed_missing = max(0, max_pages_per_site - current_scraped_count)
                        if allowed_missing is not None:
                            if allowed_missing == 0:
                                missing_normalized = []
                            elif allowed_missing < len(missing_normalized):
                                print(
                                    f"  Limiting sitemap backfill to {allowed_missing} URL(s) "
                                    "to honor max_pages_per_site"
                                )
                                missing_normalized = missing_normalized[:allowed_missing]
                        if missing_normalized:
                            backfill_urls = [normalized_map[norm] for norm in missing_normalized]
                            print(f"  ➤ Found {len(backfill_urls)} sitemap URL(s) missing from scrape history. Backfilling now...")
                            backfill_result = _scrape_url_batch_for_site(
                                backfill_urls,
                                total_urls=sitemap_total,
                                pages_processed_start=current_scraped_count,
                                label="sitemap_backfill"
                            )
                            sitemap_backfill_scraped = backfill_result["scraped"]
                            sitemap_backfill_failed = backfill_result["failed"]
                            if sitemap_backfill_scraped > 0:
                                site_result["status"] = "completed"
                            _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                        else:
                            print("  ✓ Sitemap coverage already complete for this site.")
                    else:
                        print(f"Sitemap discovered but contained no URLs for {base_domain}.")
                else:
                    print(f"No sitemap available for {base_domain}; skipping coverage comparison.")

                # Update scraped_sites metadata with latest counters
                current_active_pages = self.scraped_content_collection.count_documents(
                    {"base_domain": base_domain, "status": "active"}
                )
                site_update: Dict[str, Any] = {
                    "$set": {
                        "last_scraped_at": datetime.utcnow(),
                        "total_pages": current_active_pages
                    },
                    "$addToSet": {"modes": mode_name}
                }
                inc_doc: Dict[str, int] = {}
                if sitemap_backfill_scraped:
                    inc_doc["successful_pages"] = sitemap_backfill_scraped
                if sitemap_backfill_failed:
                    inc_doc["failed_pages"] = sitemap_backfill_failed
                if inc_doc:
                    site_update["$inc"] = inc_doc
                site_filter = {"_id": site_doc.get("_id")} if site_doc.get("_id") else {"base_domain": base_domain}
                self.scraped_sites_collection.update_one(site_filter, site_update)
                
            else:
                # Site not scraped yet - crawl and scrape it
                print(f"Site {base_domain} not yet scraped. Starting full crawl...")
                
                # Step 2: Discover URLs or resume remaining queue
                if site_resume_state and site_resume_state.get("current_site_remaining_urls") is not None:
                    urls_to_scrape = [
                        url for url in site_resume_state.get("current_site_remaining_urls", []) if url
                    ]
                    total_urls = site_resume_state.get("current_site_total_urls") or len(urls_to_scrape)
                    pages_processed = site_resume_state.get("current_site_processed_urls") or max(0, total_urls - len(urls_to_scrape))
                    print(f"Resuming crawl with {len(urls_to_scrape)} remaining URL(s) ({pages_processed}/{total_urls} processed)")
                else:
                    urls_to_scrape = self._crawl_site(
                        site_url, 
                        max_pages=max_pages_per_site,
                        max_depth=max_crawl_depth
                    )
                    total_urls = len(urls_to_scrape)
                    pages_processed = 0
                
                if not urls_to_scrape:
                    print("No URLs discovered for site. Skipping.")
                    _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                    _emit_progress({})
                    site_result["status"] = "completed"
                    results["sites"].append(site_result)
                    continue
                
                _update_checkpoint(
                    current_site=site_url,
                    remaining_urls=urls_to_scrape,
                    total_urls=total_urls,
                    processed_urls=pages_processed
                )
                
                _emit_progress({
                    "phase": "discovery_complete",
                    "current_site": base_domain,
                    "urls_discovered": total_urls,
                    "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                    "scraped_pages": results["total_pages_scraped"],
                    "reused_pages": results["total_pages_reused"],
                    "failed_pages": results["total_pages_failed"]
                })
                
                batch_result = _scrape_url_batch_for_site(
                    list(urls_to_scrape),
                                total_urls=total_urls,
                    pages_processed_start=pages_processed,
                    label="site"
                )
                scraped_count = batch_result["scraped"]
                failed_count = batch_result["failed"]
                pages_processed = batch_result["processed"]
                
                # Step 4: Record that this site has been scraped
                self.scraped_sites_collection.insert_one({
                    "base_domain": base_domain,
                    "base_url": site_url,
                    "first_scraped_at": datetime.utcnow(),
                    "last_scraped_at": datetime.utcnow(),
                    "total_pages": total_urls,
                    "successful_pages": scraped_count,
                    "failed_pages": failed_count,
                    "modes": [mode_name]
                })
                
                site_result["status"] = "completed"
                print(f"\n✓ Completed scraping {base_domain}")
                print(f"  Scraped: {scraped_count}, Reused: {site_result['pages_reused']}, Failed: {failed_count}")
                _update_checkpoint(current_site=None, pending_override=list(pending_sites_queue))
                _emit_progress({})
            
            results["sites"].append(site_result)
            
            if site_metrics and site_timer:
                self._finalize_site_metrics(site_metrics, site_timer)
                print(f"[METRICS] Site {base_domain}: {site_metrics}")
        
        # Update mode with scraping timestamp
        total_content = results["total_pages_scraped"] + results["total_pages_reused"]
        self.modes_collection.update_one(
            {"name": mode_name},
            {
                "$set": {
                    "last_scraped_at": datetime.utcnow(),
                    "has_scraped_content": total_content > 0
                }
            }
        )
        
        print(f"\n{'='*60}")
        print(f"SCRAPING COMPLETE FOR MODE '{mode_name}'")
        print(f"Total pages: {total_content} (scraped: {results['total_pages_scraped']}, reused: {results['total_pages_reused']}, failed: {results['total_pages_failed']})")
        print(f"{'='*60}\n")
        
        results["newly_scraped_content_ids"] = newly_scraped_ids
        site_content_ids: List[str] = []
        verification_domain: Optional[str] = None
        if processed_domains:
            domain_list = list(processed_domains)
            if len(domain_list) == 1:
                verification_domain = domain_list[0]
            cursor = self.scraped_content_collection.find(
                {"base_domain": {"$in": domain_list}, "status": "active"},
                {"_id": 1}
            )
            site_content_ids = [str(doc["_id"]) for doc in cursor]
        results["verification_candidate_ids"] = site_content_ids
        
        # Trigger background verification for all pages tied to the scraped sites
        if site_content_ids:
            print(f"Triggering background verification for {len(site_content_ids)} page(s) across scraped sites...")
            if self.verification_scheduler:
                try:
                    # Trigger background verification via scheduler (non-blocking)
                    job_id = self.verification_scheduler.trigger_background_verification(
                        batch_size=len(site_content_ids),
                        content_ids=site_content_ids,
                        mode_name=mode_name,
                        base_domain=verification_domain
                    )
                    print(f"✓ Background verification job started: {job_id}")
                    results["verification_job_id"] = str(job_id)
                except Exception as e:
                    print(f"⚠ Could not start background verification: {e}")
                    print(f"  Starting verification in separate thread without job tracking...")
                    self._trigger_verification_thread(len(site_content_ids), site_content_ids, mode_name=mode_name)
            else:
                print(f"⚠ No verification scheduler configured - starting verification in separate thread...")
                self._trigger_verification_thread(len(site_content_ids), site_content_ids, mode_name=mode_name)
        
        return results
    
    def _trigger_verification_thread(self, batch_size: int, content_ids: Optional[List[str]] = None, mode_name: Optional[str] = None):
        """
        Trigger verification in a separate thread when scheduler is not available.
        
        Args:
            batch_size: Number of pages to verify
        """
        import threading
        
        def run_verification():
            try:
                print(f"Background verification thread started for {batch_size} pages")
                filters: Optional[Dict[str, Any]] = None
                if content_ids or mode_name:
                    filters = {}
                    if content_ids:
                        filters["content_ids"] = content_ids
                    if mode_name:
                        filters["mode_name"] = mode_name
                result = self.verify_scraped_content(batch_size=batch_size, filters=filters)
                print(f"Background verification complete: {result.get('verified_updated', 0)} updated, {result.get('verified_unchanged', 0)} unchanged")
            except Exception as e:
                print(f"Background verification error: {e}")
        
        thread = threading.Thread(
            target=run_verification,
            daemon=True,
            name="BackgroundVerification"
        )
        thread.start()
        print(f"✓ Background verification thread started")
    
    def _compare_content(self, content1: str, content2: str, threshold: float = 0.01) -> Tuple[bool, float]:
        """
        Compare two content strings to determine if they're different.
        
        Args:
            content1: First content string
            content2: Second content string
            threshold: Difference threshold (0.01 = 1% difference, essentially any change)
            
        Returns:
            Tuple of (is_different, difference_ratio)
        """
        # Remove the Source: URL line from both for comparison
        lines1 = [line for line in content1.split('\n') if not line.startswith('Source:')]
        lines2 = [line for line in content2.split('\n') if not line.startswith('Source:')]
        
        text1 = '\n'.join(lines1)
        text2 = '\n'.join(lines2)
        
        # Compare word counts
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        # Calculate difference ratio
        if not words1 and not words2:
            return False, 0.0
        
        # Words in content2 but not in content1
        new_words = words2 - words1
        # Words in content1 but not in content2
        removed_words = words1 - words2
        
        total_unique_words = len(words1.union(words2))
        changed_words = len(new_words) + len(removed_words)
        
        if total_unique_words == 0:
            return False, 0.0
        
        difference_ratio = changed_words / total_unique_words
        
        # Check character length difference
        len_diff = abs(len(text2) - len(text1)) / max(len(text1), 1)
        
        # Consider it different if word difference OR length difference exceeds threshold
        # With threshold of 0.01 (1%), essentially any meaningful change triggers update
        is_different = difference_ratio > threshold or len_diff > threshold
        
        return is_different, max(difference_ratio, len_diff)
    
    def verify_scraped_content(
        self,
        batch_size: int = 10,
        max_retries: int = 2,
        progress_callback=None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, any]:
        """
        Background process to verify scraped content by re-scraping with full dynamic content.
        Compares the dynamic scrape with the initial fast scrape and updates if different.
        
        Args:
            batch_size: Number of pages to verify in this run
            max_retries: Maximum retry attempts for failed scrapes
            progress_callback: Optional callback function(progress_dict)
            
        Returns:
            Dictionary with verification results
        """
        # Find content that needs verification
        query: Dict[str, Any] = {
            "verification_status": "pending_verification",
            "status": "active"
        }
        limit = batch_size
        if filters:
            mode_filter = filters.get("mode_name")
            if mode_filter:
                query["modes"] = mode_filter
            content_ids = filters.get("content_ids")
            if content_ids:
                from bson import ObjectId
                object_ids = [ObjectId(cid) for cid in content_ids if cid]
                if object_ids:
                    query["_id"] = {"$in": object_ids}
                    limit = len(object_ids)
        pending_content = list(self.scraped_content_collection.find(query).limit(limit))
        
        if not pending_content:
            return {
                "success": True,
                "message": "No content pending verification",
                "verified": 0,
                "updated": 0,
                "failed": 0
            }
        
        results = {
            "success": True,
            "total_checked": len(pending_content),
            "verified_unchanged": 0,
            "verified_updated": 0,
            "failed": 0,
            "updates": []
        }
        
        print(f"\n{'='*60}")
        print(f"STARTING CONTENT VERIFICATION")
        print(f"Checking {len(pending_content)} pages for dynamic content")
        print(f"{'='*60}\n")
        
        # Create a single browser instance for all verifications (significant performance boost)
        print(f"Creating browser instance for verifying {len(pending_content)} pages...")
        from playwright.sync_api import sync_playwright
        p = self._start_playwright()
        browser = p.chromium.launch(headless=True)
        
        try:
            for idx, doc in enumerate(pending_content, 1):
                url = doc.get("original_url")
                normalized_url = doc.get("normalized_url")
                original_content = doc.get("content", "")
                base_domain = doc.get("base_domain")
                doc_modes = doc.get("modes", [])
                primary_mode = doc_modes[0] if doc_modes else None
                
                print(f"\n[{idx}/{len(pending_content)}] Verifying: {url}")
                
                retry_count = 0
                success = False
                error = None
                
                while retry_count <= max_retries and not success:
                    # Second pass: Hybrid scrape - static first, then merge dynamic content
                    # File discovery happens ONLY in second pass (embedded PDFs, downloadable files, etc.)
                    # Pass the reusable browser instance for performance
                    content, title, error, html_content, file_links = self.scrape_url(
                        url,
                        extract_files=True,  # Second pass: extract all discoverable files
                        merge_dynamic_content=True,  # Use merge mode for second pass
                        expand_accordions=True,
                        playwright_browser=browser,  # Reuse browser for performance
                        embedded_pdf_checks=False,
                    )
                    
                    if error:
                        retry_count += 1
                        if retry_count <= max_retries:
                            print(f"  ↻ Retry {retry_count}/{max_retries}")
                            time.sleep(2 ** retry_count)
                        continue
                    
                    success = True
                    
                    # Compare content
                    is_different, diff_ratio = self._compare_content(original_content, content)
                    
                    if is_different:
                        print(f"  🔄 Content differs by {diff_ratio*100:.1f}% - Updating...")
                        
                        # Re-upload to vector store with updated content
                        scraped_at = datetime.utcnow()
                        
                        # Delete old file from OpenAI if it exists
                        old_file_id = doc.get("openai_file_id")
                        if old_file_id and self.vector_store_id:
                            try:
                                self.client.files.delete(old_file_id)
                                self.client.vector_stores.files.delete(
                                    vector_store_id=self.vector_store_id,
                                    file_id=old_file_id
                                )
                            except Exception as e:
                                print(f"  ⚠ Warning: Could not delete old file: {e}")
                        
                        # Upload new content
                        # Get first mode from the modes list for upload
                        # Use the original title from first pass for consistency
                        mode_name = doc.get("modes", ["default"])[0]
                        original_title = doc.get("title", title)  # Fallback to new title if not found
                        openai_file_id = self.upload_to_vector_store(
                            content, mode_name, url, original_title, scraped_at
                        )
                        
                        # Update MongoDB with new content
                        # Note: We preserve the original title from the first pass
                        # since it was captured immediately after page load
                        update_data = {
                            "$set": {
                                "content": content,
                                # "title": title,  # Keep original title from first pass
                                "verified_at": scraped_at,
                                "verification_status": "verified_updated",
                                "openai_file_id": openai_file_id,
                                "metadata.word_count": len(content.split()),
                                "metadata.char_count": len(content),
                                "metadata.content_difference": diff_ratio
                            }
                        }
                        
                        self.scraped_content_collection.update_one(
                            {"_id": doc["_id"]},
                            update_data
                        )
                        
                        # Store newly discovered files
                        if file_links:
                            for file_info in file_links:
                                for mode in doc.get("modes", []):
                                    file_info_copy = file_info.copy()
                                    file_info_copy["mode"] = mode
                                    file_info_copy["discovered_at"] = scraped_at
                                    file_info_copy["user_id"] = doc.get("user_id")
                                    file_info_copy["status"] = "discovered"
                                    file_info_copy["base_domain"] = doc.get("base_domain")
                                    self._insert_discovered_file(file_info_copy)
                        
                        results["verified_updated"] += 1
                        results["updates"].append({
                            "url": url,
                            "difference": f"{diff_ratio*100:.1f}%",
                            "old_words": len(original_content.split()),
                            "new_words": len(content.split())
                        })
                        
                        print(f"  ✓ Updated ({len(content.split())} words, +{len(content.split()) - len(original_content.split())} words)")
                        
                    else:
                        print(f"  ✓ Content unchanged (diff: {diff_ratio*100:.1f}%)")
                        
                        # Mark as verified (no update needed)
                        self.scraped_content_collection.update_one(
                            {"_id": doc["_id"]},
                            {
                                "$set": {
                                    "verified_at": datetime.utcnow(),
                                    "verification_status": "verified_unchanged"
                                }
                            }
                        )
                        
                        results["verified_unchanged"] += 1
                    
                    # Update progress
                    if progress_callback:
                        progress_callback({
                            "current_page": idx,
                            "total_pages": len(pending_content),
                            "url": url,
                            "base_domain": base_domain,
                            "modes": doc_modes,
                            "primary_mode": primary_mode,
                            "verified_unchanged": results["verified_unchanged"],
                            "verified_updated": results["verified_updated"],
                            "failed": results["failed"]
                        })
                    
                    # Rate limiting
                    time.sleep(1)
                
                if not success:
                    print(f"  ✗ Verification failed: {error}")
                    
                    # Mark as failed verification (keep original content)
                    self.scraped_content_collection.update_one(
                        {"_id": doc["_id"]},
                        {
                            "$set": {
                                "verification_status": "verification_failed",
                                "verification_error": error,
                                "verification_attempted_at": datetime.utcnow()
                            }
                        }
                    )
                    
                    results["failed"] += 1
                    
                    if progress_callback:
                        progress_callback({
                            "current_page": idx,
                            "total_pages": len(pending_content),
                            "url": url,
                            "base_domain": base_domain,
                            "modes": doc_modes,
                            "primary_mode": primary_mode,
                            "verified_unchanged": results["verified_unchanged"],
                            "verified_updated": results["verified_updated"],
                            "failed": results["failed"]
                        })
        finally:
            # Always close the browser when done with verification
            print(f"Closing browser instance...")
            browser.close()
            p.stop()
        
        print(f"\n{'='*60}")
        print(f"VERIFICATION COMPLETE")
        print(f"Checked: {results['total_checked']}, Unchanged: {results['verified_unchanged']}, Updated: {results['verified_updated']}, Failed: {results['failed']}")
        print(f"{'='*60}\n")
        
        return results
    
    def get_verification_statistics(self, mode_name: Optional[str] = None) -> Dict[str, any]:
        """
        Get statistics about content verification status.
        
        Args:
            mode_name: Optional mode name to filter statistics
            
        Returns:
            Dictionary with verification statistics
        """
        query = {"status": "active"}
        if mode_name:
            query["modes"] = mode_name
        
        # Count by verification status
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": "$verification_status",
                    "count": {"$sum": 1}
                }
            }
        ]
        
        results = list(self.scraped_content_collection.aggregate(pipeline))
        
        stats = {
            "pending_verification": 0,
            "verified_unchanged": 0,
            "verified_updated": 0,
            "verification_failed": 0,
            "total": 0
        }
        
        for result in results:
            status = result.get("_id") or "pending_verification"  # Default for old docs
            count = result.get("count", 0)
            stats[status] = count
            stats["total"] += count
        
        return stats
    
    def delete_scraped_content(self, content_id: str, mode_name: Optional[str] = None) -> bool:
        """
        Delete scraped content from both MongoDB and vector store.
        If content is used by multiple modes and mode_name is specified,
        only removes the mode association. Otherwise, deletes entirely.
        
        Args:
            content_id: MongoDB document ID
            mode_name: Optional mode name to unlink (keeps content if used by other modes)
            
        Returns:
            True if successful, False otherwise
        """
        from bson import ObjectId
        
        try:
            # Get the document
            doc = self.scraped_content_collection.find_one({"_id": ObjectId(content_id)})
            if not doc:
                return False
            
            modes_list = doc.get("modes", [])
            
            # If mode_name specified and content is used by multiple modes, just unlink
            if mode_name and len(modes_list) > 1:
                self.scraped_content_collection.update_one(
                    {"_id": ObjectId(content_id)},
                    {"$pull": {"modes": mode_name}}
                )
                print(f"Unlinked content from mode '{mode_name}' (still used by other modes)")
                
                # Update mode's has_scraped_content flag
                remaining_count = self.scraped_content_collection.count_documents({
                    "modes": mode_name,
                    "status": "active"
                })
                if remaining_count == 0:
                    self.modes_collection.update_one(
                        {"name": mode_name},
                        {"$set": {"has_scraped_content": False}}
                    )
                
                return True
            
            # Otherwise, delete the content entirely
            # Delete from OpenAI
            openai_file_id = doc.get("openai_file_id")
            if openai_file_id and self.vector_store_id:
                try:
                    self.client.files.delete(openai_file_id)
                    self.client.vector_stores.files.delete(
                        vector_store_id=self.vector_store_id,
                        file_id=openai_file_id
                    )
                except Exception as e:
                    print(f"Error deleting from OpenAI: {e}")
            
            # Delete from MongoDB
            self.scraped_content_collection.delete_one({"_id": ObjectId(content_id)})
            
            # Update all affected modes
            for mode in modes_list:
                remaining_count = self.scraped_content_collection.count_documents({
                    "modes": mode,
                    "status": "active"
                })
                if remaining_count == 0:
                    self.modes_collection.update_one(
                        {"name": mode},
                        {"$set": {"has_scraped_content": False}}
                    )
            
            return True
            
        except Exception as e:
            print(f"Error deleting scraped content: {e}")
            return False

