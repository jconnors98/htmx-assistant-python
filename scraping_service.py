"""
Web scraping service for extracting content from configured websites.
Uses Playwright for dynamic content handling (accordions, tabs, etc.)
"""

import io
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


class ScrapingService:
    """Service for scraping websites and managing scraped content."""

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
        self.modes_collection = mongo_db.get_collection("modes")
        self.vector_store_id = vector_store_id
        self.local_dev_mode = os.getenv("LOCAL_DEV_MODE", "false").lower() == "true"
        self.verification_scheduler = None  # Will be set by scheduler if needed
        
        # Create indexes for efficient lookups
        try:
            self.scraped_content_collection.create_index("normalized_url", unique=True)
            self.scraped_content_collection.create_index("base_domain")
            self.scraped_content_collection.create_index("verification_status")
            self.scraped_sites_collection.create_index("base_domain", unique=True)
            self.discovered_files_collection.create_index([("mode", 1), ("file_url", 1)], unique=True)
            self.discovered_files_collection.create_index("mode")
        except Exception:
            pass  # Indexes may already exist
        
    def scrape_url(
        self, 
        url: str, 
        expand_accordions: bool = False,
        timeout: int = 30000,
        extract_files: bool = False,
        load_dynamic_content: bool = False,
        merge_dynamic_content: bool = False,
        playwright_browser=None
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
            # Validate URL
            if not self._is_valid_url(url):
                return "", "", f"Invalid URL: {url}", None, None
            
            # Reuse browser if provided, otherwise create new one
            if playwright_browser:
                browser = playwright_browser
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                should_close_browser = False
            else:
                p = sync_playwright().start()
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                should_close_browser = True
            
            try:
                # Navigate to URL and wait for content to load
                # Use 'domcontentloaded' for fast scraping, 'networkidle' for dynamic content
                wait_until = "networkidle" if load_dynamic_content else "domcontentloaded"
                page.goto(url, timeout=timeout, wait_until=wait_until)
                
                # IMPORTANT: Capture title immediately after navigation, before any interactions
                # This ensures we get the original page title before any dynamic changes or navigation
                initial_title = page.title() or "Untitled"
                
                pass_number = 1
                
                # Hybrid approach for second pass: scrape static first, then add dynamic content
                if merge_dynamic_content:
                    pass_number = 2
                    print(f"PASS {pass_number} - Starting hybrid scrape (static + dynamic merge)")
                    
                    # Step 1: Get initial static content
                    page.wait_for_timeout(500)  # Brief wait for basic content
                    initial_html = page.content()
                    initial_content = self._extract_clean_text(initial_html, url)
                    initial_elements = self._get_text_elements(page)
                    print(f"  Initial content: {len(initial_content)} chars, {len(initial_elements)} elements")
                    
                    # Step 2: Wait for and expand dynamic content (without navigation)
                    self._wait_for_dynamic_content(page)
                    if expand_accordions:
                        self._expand_dynamic_elements_safe(page, url)
                    
                    # Step 3: Get content after expansion
                    expanded_html = page.content()
                    expanded_content = self._extract_clean_text(expanded_html, url)
                    expanded_elements = self._get_text_elements(page)
                    print(f"  Expanded content: {len(expanded_content)} chars, {len(expanded_elements)} elements")
                    
                    # Step 4: Merge new content into original
                    content = self._merge_content(initial_content, expanded_content, initial_elements, expanded_elements)
                    html_content = expanded_html
                    title = initial_title  # Use the title captured at initial page load
                    
                    print(f"  Final merged content: {len(content)} chars")
                    print(f"PASS {pass_number} TITLE: {title}")
                    
                # Original logic for load_dynamic_content (without merge)
                elif load_dynamic_content:
                    pass_number = 2
                    self._wait_for_dynamic_content(page)
                    
                    # Expand dynamic content if requested
                    if expand_accordions:
                        self._expand_dynamic_elements(page)
                    
                    # Get page content
                    html_content = page.content()
                    content = self._extract_clean_text(html_content, url)
                    title = initial_title  # Use the title captured at initial page load
                    print(f"PASS {pass_number} TITLE: {title}")
                    
                else:
                    # Fast mode: just a brief wait to ensure basic content is loaded
                    page.wait_for_timeout(500)
                    html_content = page.content()
                    content = self._extract_clean_text(html_content, url)
                    title = initial_title  # Use the title captured at initial page load
                    print(f"PASS {pass_number} TITLE: {title}")
                
                print(f"PASS {pass_number} CONTENT LENGTH: {len(content)} chars")
                
                # Extract file links if requested
                file_links = None
                if extract_files:
                    file_links = self._extract_file_links(html_content, url, title, playwright_browser=browser)
                
                # Close context but keep browser if reusing
                context.close()
                if should_close_browser:
                    browser.close()
                    if 'p' in locals():
                        p.stop()
                
                if not content or len(content.strip()) < 100:
                    return "", title, "Insufficient content extracted (less than 100 characters)", html_content, file_links
                
                return content, title, None, html_content, file_links
                
            except PlaywrightTimeoutError:
                context.close()
                if should_close_browser:
                    browser.close()
                    if 'p' in locals():
                        p.stop()
                return "", "", f"Timeout loading page: {url}", None, None
            except Exception as e:
                context.close()
                if should_close_browser:
                    browser.close()
                    if 'p' in locals():
                        p.stop()
                return "", "", f"Error loading page: {str(e)}", None, None
                    
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
            
            print(f"SCRAPING: Waiting for loading indicators to disappear")
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
            print("SCRAPING:Scrolling to bottom to trigger lazy loading")
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
            print("SCRAPING: Waiting for DOM to stabilize")
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
            print("SCRAPING: Waiting for final rendering to complete")
            page.wait_for_timeout(1000)
            
            # Step 6: Wait for images to load (optional, but helps with complete rendering)
            print("SCRAPING: Waiting for images to load")
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
                    print(f"SCRAPING: Expanding {len(elements)} elements matching '{selector}'")
                    for element in elements:  # Expand ALL elements, no limit
                        try:
                            if element.is_visible():
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
                    response = requests.get(sitemap_url, timeout=10)
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
            response = requests.get(sitemap_url, timeout=10)
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
            # Reuse browser if provided, otherwise create new one
            if playwright_browser:
                browser = playwright_browser
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                should_close_browser = False
            else:
                p = sync_playwright().start()
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                should_close_browser = True
            
            try:
                # Navigate to viewer page
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
                                    const match = onclick.match(/['"]([^'"]*\.pdf[^'"]*)['"]/);
                                    if (match) return match[1];
                                }
                            }
                            
                            return null;
                        }
                """)
                
                # Close context but keep browser if reusing
                context.close()
                if should_close_browser:
                    browser.close()
                    if 'p' in locals():
                        p.stop()
                
                if pdf_url:
                    # Make URL absolute if relative
                    absolute_pdf_url = urljoin(viewer_url, pdf_url)
                    
                    # Extract filename from URL
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
                else:
                    return None
                    
            except Exception as e:
                context.close()
                if should_close_browser:
                    browser.close()
                    if 'p' in locals():
                        p.stop()
                print(f"  ⚠ Error extracting PDF from viewer: {e}")
                return None
                    
        except Exception as e:
            print(f"  ⚠ Browser error while extracting PDF: {e}")
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
                p = sync_playwright().start()
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
        except Exception as e:
            print(f"  ⚠ Error in embedded PDF check: {e}")
            return None
    
    def _extract_file_links(self, html_content: str, base_url: str, source_page_title: str, playwright_browser=None) -> List[Dict]:
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
        # Downloadable file extensions to look for
        FILE_EXTENSIONS = {
            'pdf': 'PDF Document',
            'doc': 'Word Document',
            'docx': 'Word Document',
            'xls': 'Excel Spreadsheet',
            'xlsx': 'Excel Spreadsheet',
            'ppt': 'PowerPoint Presentation',
            'pptx': 'PowerPoint Presentation',
            'txt': 'Text File',
            'csv': 'CSV File',
            'zip': 'ZIP Archive',
            'rar': 'RAR Archive',
            'json': 'JSON File',
            'xml': 'XML File'
        }
        
        files = []
        pdf_viewer_urls = []  # Track potential PDF viewer URLs to check
        embed_check_urls = []  # Track URLs to check for embedded PDFs
        seen_file_urls = set()  # Track seen file URLs to prevent duplicates
        
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
                
                # Parse URL to get file extension
                parsed = urlparse(absolute_url)
                path = parsed.path.lower()
                
                # Check if URL points to a downloadable file
                file_ext = None
                for ext, file_type in FILE_EXTENSIONS.items():
                    if path.endswith(f'.{ext}'):
                        file_ext = ext
                        break
                
                # Get link text for context
                link_text = link.get_text(strip=True)
                
                if file_ext:
                    # Direct file link found
                    # Normalize URL for duplicate checking
                    normalized_file_url = self._normalize_url(absolute_url)
                    
                    # Skip if we've already seen this file URL
                    if normalized_file_url in seen_file_urls:
                        continue
                    
                    filename = os.path.basename(parsed.path)
                    
                    # Get file size if available in link text
                    file_size = None
                    size_match = re.search(r'[\(\[]?\s*(\d+\.?\d*\s*(?:KB|MB|GB))\s*[\)\]]?', link_text, re.IGNORECASE)
                    if size_match:
                        file_size = size_match.group(1)
                    
                    files.append({
                        'file_url': absolute_url,
                        'filename': filename,
                        'file_type': FILE_EXTENSIONS[file_ext],
                        'file_extension': file_ext,
                        'link_text': link_text or filename,
                        'file_size': file_size,
                        'source_page_url': base_url,
                        'source_page_title': source_page_title
                    })
                    
                    # Mark this file URL as seen
                    seen_file_urls.add(normalized_file_url)
                    
                elif self._is_pdf_viewer_page(absolute_url):
                    # Potential PDF viewer page - add to list for checking
                    pdf_viewer_urls.append((absolute_url, link_text))
                else:
                    # Check if this link might lead to a page with an embedded PDF
                    # Only check links from the same domain to avoid excessive crawling
                    if self._is_same_domain(absolute_url, base_url):
                        embed_check_urls.append((absolute_url, link_text))
            
            # Process PDF viewer URLs (check all for thoroughness)
            if pdf_viewer_urls:
                duplicates_skipped = 0
                
                for viewer_url, link_text in pdf_viewer_urls:  # Check ALL viewers, no limit
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
                
                if duplicates_skipped > 0:
                    print(f"  ⏭️  Skipped {duplicates_skipped} duplicate PDF(s) from viewers")
            
            # Process URLs that might have embedded PDFs (check all links for thoroughness)
            if embed_check_urls:
                duplicates_skipped = 0
                
                for embed_url, link_text in embed_check_urls:  # Check ALL links, no limit
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
                
                if duplicates_skipped > 0:
                    print(f"  ⏭️  Skipped {duplicates_skipped} duplicate embedded PDF(s)")
            
        except Exception as e:
            print(f"Error extracting file links: {e}")
        
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
                        response = requests.get(current_url, timeout=10, headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                        })
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
        progress_callback=None
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
        
        results = {
            "success": True,
            "mode": mode_name,
            "total_sites": len(scrape_sites),
            "total_pages_scraped": 0,
            "total_pages_reused": 0,
            "total_pages_failed": 0,
            "sites": []
        }
        
        for site_url in scrape_sites:
            site_url = site_url.strip()
            if not site_url:
                continue
            
            base_domain = self._get_base_domain(site_url)
            is_single_page = self._is_single_page_url(site_url)
            
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
            
            # If it's a single page, scrape only that page
            if is_single_page:
                print(f"Scraping single page: {site_url}")
                
                normalized_url = self._normalize_url(site_url)
                
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
                    site_result["pages_reused"] = 1
                    results["total_pages_reused"] += 1
                    site_result["status"] = "reused"
                    print(f"  ✓ Reused existing content")
                    
                    # Update progress
                    if progress_callback:
                        progress_callback({
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
                        # First pass: Fast scrape without dynamic content (no file discovery)
                        content, title, error, html_content, file_links = self.scrape_url(
                            site_url, 
                            extract_files=False,  # First pass: content only, no file discovery
                            load_dynamic_content=False
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
                        
                        self.scraped_content_collection.insert_one(content_doc)
                        
                        # Store discovered files
                        if file_links:
                            for file_info in file_links:
                                file_info["mode"] = mode_name
                                file_info["discovered_at"] = scraped_at
                                file_info["user_id"] = user_id
                                file_info["status"] = "discovered"
                                file_info["base_domain"] = base_domain
                                
                                try:
                                    self.discovered_files_collection.insert_one(file_info)
                                except Exception as e:
                                    pass
                            
                            print(f"  📄 Discovered {len(file_links)} downloadable file(s)")
                        
                        site_result["pages_scraped"] = 1
                        results["total_pages_scraped"] += 1
                        site_result["status"] = "completed"
                        success = True
                        print(f"  ✓ Successfully scraped ({content_doc['metadata']['word_count']} words)")
                        
                        # Update progress
                        if progress_callback:
                            progress_callback({
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
                        
                        # Update progress
                        if progress_callback:
                            progress_callback({
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
                    reused_count += 1
                
                site_result["pages_reused"] = reused_count
                site_result["status"] = "reused"
                results["total_pages_reused"] += reused_count
                
                print(f"Reused {reused_count} existing pages from {base_domain}")
                
                # Update progress
                if progress_callback:
                    progress_callback({
                        "current_site": base_domain,
                        "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                        "scraped_pages": results["total_pages_scraped"],
                        "reused_pages": results["total_pages_reused"],
                        "failed_pages": results["total_pages_failed"]
                    })
                
            else:
                # Site not scraped yet - crawl and scrape it
                print(f"Site {base_domain} not yet scraped. Starting full crawl...")
                
                # Step 2: Discover all URLs on the site
                urls_to_scrape = self._crawl_site(
                    site_url, 
                    max_pages=max_pages_per_site,
                    max_depth=max_crawl_depth
                )
                
                # Notify about URLs discovered before scraping starts
                if progress_callback:
                    progress_callback({
                        "phase": "discovery_complete",
                        "current_site": base_domain,
                        "urls_discovered": len(urls_to_scrape),
                        "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                        "scraped_pages": results["total_pages_scraped"],
                        "reused_pages": results["total_pages_reused"],
                        "failed_pages": results["total_pages_failed"]
                    })
                
                # Step 3: Scrape each discovered URL
                # Create a single browser instance for all pages in this site (significant performance boost)
                print(f"Creating browser instance for scraping {len(urls_to_scrape)} pages...")
                from playwright.sync_api import sync_playwright
                p = sync_playwright().start()
                browser = p.chromium.launch(headless=True)
                
                scraped_count = 0
                failed_count = 0
                processed_urls = set()  # Track URLs processed in this session to prevent duplicates
                
                try:
                    for idx, url in enumerate(urls_to_scrape, 1):
                        normalized_url = self._normalize_url(url)
                        
                        # Skip if we've already processed this URL in this session
                        if normalized_url in processed_urls:
                            print(f"\nSkipping duplicate URL (already processed): {url}")
                            continue
                        
                        processed_urls.add(normalized_url)
                        print(f"\nScraping page {idx}/{len(urls_to_scrape)}: {url}")
                        
                        # Check if this specific URL was already scraped
                        existing = self.scraped_content_collection.find_one({
                            "normalized_url": normalized_url
                        })
                        
                        if existing:
                            # URL already exists, just link to mode
                            self.scraped_content_collection.update_one(
                                {"_id": existing["_id"]},
                                {"$addToSet": {"modes": mode_name}}
                            )
                            site_result["pages_reused"] += 1
                            results["total_pages_reused"] += 1
                            print(f"  ✓ Reused existing content")
                            
                            # Update progress after reusing page
                            if progress_callback:
                                progress_callback({
                                    "current_site": base_domain,
                                    "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                                    "scraped_pages": results["total_pages_scraped"],
                                    "reused_pages": results["total_pages_reused"],
                                    "failed_pages": results["total_pages_failed"]
                                })
                            continue
                        
                        # Scrape the URL with retries (first pass: fast scrape)
                        retry_count = 0
                        success = False
                        
                        while retry_count <= max_retries and not success:
                            # First pass: Fast scrape without dynamic content (no file discovery)
                            # Pass the reusable browser instance for performance
                            content, title, error, html_content, file_links = self.scrape_url(
                                url, 
                                extract_files=False,  # First pass: content only, no file discovery
                                load_dynamic_content=False,
                                playwright_browser=browser  # Reuse browser for performance
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
                                content, mode_name, url, title, scraped_at
                            )
                            
                            # Save to MongoDB with verification status
                            content_doc = {
                                "normalized_url": normalized_url,
                                "original_url": url,
                                "base_domain": base_domain,
                                "title": title,
                                "content": content,
                                "scraped_at": scraped_at,
                                "openai_file_id": openai_file_id,
                                "status": "active",
                                "verification_status": "pending_verification",  # Track verification status
                                "modes": [mode_name],  # List of modes using this content
                                "user_id": user_id,
                                "metadata": {
                                    "word_count": len(content.split()),
                                    "char_count": len(content)
                                }
                            }
                            
                            self.scraped_content_collection.insert_one(content_doc)
                            
                            # Store discovered files
                            if file_links:
                                for file_info in file_links:
                                    file_info["mode"] = mode_name
                                    file_info["discovered_at"] = scraped_at
                                    file_info["user_id"] = user_id
                                    file_info["status"] = "discovered"  # 'discovered' or 'added'
                                    file_info["base_domain"] = base_domain
                                    
                                    try:
                                        self.discovered_files_collection.insert_one(file_info)
                                    except Exception as e:
                                        # Duplicate file URL for this mode - skip
                                        pass
                                
                                print(f"  📄 Discovered {len(file_links)} downloadable file(s)")
                            
                            scraped_count += 1
                            site_result["pages_scraped"] += 1
                            results["total_pages_scraped"] += 1
                            success = True
                            print(f"  ✓ Successfully scraped ({content_doc['metadata']['word_count']} words)")
                            
                            # Update progress after each page
                            if progress_callback:
                                progress_callback({
                                    "current_site": base_domain,
                                    "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                                    "scraped_pages": results["total_pages_scraped"],
                                    "reused_pages": results["total_pages_reused"],
                                    "failed_pages": results["total_pages_failed"]
                                })
                            
                            # Rate limiting
                            time.sleep(1)
                        
                        if not success:
                            failed_count += 1
                            site_result["pages_failed"] += 1
                            results["total_pages_failed"] += 1
                            print(f"  ✗ Failed: {error}")
                        
                        # Update progress after failed page
                        if progress_callback:
                            progress_callback({
                                "current_site": base_domain,
                                "total_pages": results["total_pages_scraped"] + results["total_pages_reused"],
                                "scraped_pages": results["total_pages_scraped"],
                                "reused_pages": results["total_pages_reused"],
                                "failed_pages": results["total_pages_failed"]
                            })
                finally:
                    # Always close the browser when done with this site
                    print(f"Closing browser instance...")
                    browser.close()
                    p.stop()
                
                # Step 4: Record that this site has been scraped
                self.scraped_sites_collection.insert_one({
                    "base_domain": base_domain,
                    "base_url": site_url,
                    "first_scraped_at": datetime.utcnow(),
                    "last_scraped_at": datetime.utcnow(),
                    "total_pages": len(urls_to_scrape),
                    "successful_pages": scraped_count,
                    "failed_pages": failed_count,
                    "modes": [mode_name]
                })
                
                site_result["status"] = "completed"
                print(f"\n✓ Completed scraping {base_domain}")
                print(f"  Scraped: {scraped_count}, Reused: {site_result['pages_reused']}, Failed: {failed_count}")
            
            results["sites"].append(site_result)
        
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
        
        # Trigger background verification for newly scraped content
        if results["total_pages_scraped"] > 0:
            print(f"Triggering background verification for {results['total_pages_scraped']} newly scraped pages...")
            if self.verification_scheduler:
                try:
                    # Trigger background verification via scheduler (non-blocking)
                    job_id = self.verification_scheduler.trigger_background_verification(
                        batch_size=results["total_pages_scraped"]
                    )
                    print(f"✓ Background verification job started: {job_id}")
                    results["verification_job_id"] = str(job_id)
                except Exception as e:
                    print(f"⚠ Could not start background verification: {e}")
                    print(f"  Starting verification in separate thread without job tracking...")
                    self._trigger_verification_thread(results["total_pages_scraped"])
            else:
                print(f"⚠ No verification scheduler configured - starting verification in separate thread...")
                self._trigger_verification_thread(results["total_pages_scraped"])
        
        return results
    
    def _trigger_verification_thread(self, batch_size: int):
        """
        Trigger verification in a separate thread when scheduler is not available.
        
        Args:
            batch_size: Number of pages to verify
        """
        import threading
        
        def run_verification():
            try:
                print(f"Background verification thread started for {batch_size} pages")
                result = self.verify_scraped_content(batch_size=batch_size)
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
        progress_callback=None
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
        pending_content = list(self.scraped_content_collection.find({
            "verification_status": "pending_verification",
            "status": "active"
        }).limit(batch_size))
        
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
        p = sync_playwright().start()
        browser = p.chromium.launch(headless=True)
        
        try:
            for idx, doc in enumerate(pending_content, 1):
                url = doc.get("original_url")
                normalized_url = doc.get("normalized_url")
                original_content = doc.get("content", "")
                
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
                        playwright_browser=browser  # Reuse browser for performance
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
                                    
                                    try:
                                        self.discovered_files_collection.insert_one(file_info_copy)
                                    except Exception:
                                        pass  # Duplicate - skip
                        
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

