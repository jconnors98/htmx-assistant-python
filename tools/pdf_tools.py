from __future__ import annotations

import multiprocessing as mp
import time
from typing import Dict, List, Optional

import pdfplumber  # type: ignore


def _parse_pdf_inline(file_path: str, max_seconds: Optional[int]) -> Dict[str, List[str]]:
    """Internal parsing logic that respects a soft wall-clock limit."""
    text_chunks: List[str] = []
    extracted_images: List[str] = []
    truncated = False
    start = time.time()

    with pdfplumber.open(file_path) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            if max_seconds and (time.time() - start) > max_seconds:
                truncated = True
                break

            try:
                text = page.extract_text() or ""
                if text.strip():
                    text_chunks.append(text.strip())
            except Exception:  # noqa: BLE001
                continue

            if page.images:
                extracted_images.extend(
                    [
                        f"page:{page_index}:bbox({img.get('x0')},{img.get('top')},{img.get('x1')},{img.get('bottom')})"
                        for img in page.images
                    ]
                )

    return {
        "text": "\n\n".join(text_chunks),
        "images": extracted_images,
        "truncated": truncated,
    }


def _worker(file_path: str, max_seconds: Optional[int], queue):
    try:
        queue.put(_parse_pdf_inline(file_path, max_seconds))
    except Exception as exc:  # noqa: BLE001
        queue.put({"error": str(exc), "text": "", "images": [], "truncated": True})


def parse_pdf(file_path: str, *, max_seconds: Optional[int] = 90) -> Dict[str, List[str]]:
    """
    Extracts text (and basic image references) from a PDF file using pdfplumber.
    Runs parsing in a worker process with a wall-clock timeout to avoid hard freezes.
    Returns a dictionary with `text`, `images`, and `truncated` (bool when time exceeded or error).
    """
    if not max_seconds:
        return _parse_pdf_inline(file_path, None)

    queue: mp.Queue = mp.Queue()
    proc = mp.Process(target=_worker, args=(file_path, max_seconds, queue))
    proc.start()
    proc.join(timeout=max_seconds)

    if proc.is_alive():
        proc.terminate()
        proc.join()
        return {"text": "", "images": [], "truncated": True}

    if not queue.empty():
        result = queue.get()
        if result.get("error"):
            return {"text": "", "images": [], "truncated": True}
        return result

    return {"text": "", "images": [], "truncated": True}

