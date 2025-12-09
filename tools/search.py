from __future__ import annotations

from typing import Any, Dict, List, Optional


def search_documents(query: str, documents: List[Dict[str, Any]], *, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Performs a lightweight ranked search over the supplied documents. Scores are
    calculated via simple keyword overlaps so the function works without an
    external vector database.
    """
    normalized_query = (query or "").strip().lower()
    if not normalized_query:
        return []

    filter_trade = (filters or {}).get("trade")
    filter_topic = (filters or {}).get("topic")

    results: List[Dict[str, Any]] = []
    for document in documents:
        text = " ".join(
            str(document.get(field, ""))
            for field in ["raw_text", "ocr_text", "original_filename"]
        ).lower()

        if filter_trade and filter_trade not in document.get("trade_tags", []):
            continue
        if filter_topic and filter_topic not in document.get("topics", []):
            continue

        score = _score(normalized_query, text, document)
        if score <= 0:
            continue

        results.append(
            {
                "score": score,
                "document": document,
            }
        )

    return sorted(results, key=lambda item: item["score"], reverse=True)


def _score(query: str, text: str, document: Dict[str, Any]) -> float:
    score = 0.0
    for token in query.split():
        if token in text:
            score += 1.0
    if document.get("trade_tags"):
        overlap = len(set(query.split()) & set(" ".join(document["trade_tags"]).split()))
        score += overlap * 0.5
    return score

