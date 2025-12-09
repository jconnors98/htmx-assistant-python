from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Dict, List, Optional


TRADE_KEYWORDS = {
    "electrical": ["electrical", "lighting", "panel", "breaker", "circuit"],
    "mechanical": ["mechanical", "hvac", "duct", "air handling", "plumbing"],
    "civil": ["civil", "sitework", "grading", "utilities"],
    "architectural": ["architectural", "finish", "elevation", "plan"],
    "structural": ["structural", "beam", "column", "foundation", "load"],
}

TOPIC_KEYWORDS = {
    "lighting": ["light fixture", "luminaire", "lighting"],
    "fire alarm": ["fire alarm", "smoke detector", "pull station"],
    "plumbing fixtures": ["sink", "lavatory", "water closet", "fixture"],
    "windows": ["window", "glazing", "storefront"],
    "doors": ["door", "hardware", "frame"],
    "sitework": ["grading", "paving", "curb"],
}

DRAWING_TYPES = {
    "plan": ["plan", "floor plan"],
    "elevation": ["elevation"],
    "section": ["section"],
    "detail": ["detail"],
    "schedule": ["schedule"],
    "notes": ["general notes", "note"],
}

CSI_DIVISIONS = {
    3: ["concrete"],
    5: ["steel", "metal"],
    6: ["wood"],
    8: ["door", "window"],
    9: ["finish", "paint", "gypsum"],
    21: ["fire suppression", "sprinkler"],
    22: ["plumbing"],
    23: ["hvac", "mechanical"],
    26: ["electrical", "power"],
}


def classify_document(file_text: str, filename: str, ocr_text: Optional[str] = None) -> Dict[str, List[str]]:
    text = " ".join(
        part for part in [file_text or "", ocr_text or "", filename or ""] if part
    ).lower()

    trades = _match_keywords(text, TRADE_KEYWORDS)
    topics = _match_keywords(text, TOPIC_KEYWORDS)
    drawing_types = _match_keywords(text, DRAWING_TYPES)
    divisions = _detect_divisions(text)

    is_drawing = any(keyword in filename.lower() for keyword in ["plan", "elevation", "detail"])
    is_spec = filename.lower().endswith(".pdf") and not is_drawing

    return {
        "trade_tags": trades,
        "topics": topics,
        "drawing_types": drawing_types,
        "division_tags": divisions,
        "is_drawing": is_drawing,
        "is_spec": is_spec,
    }


def structured_extract(file_text: str, ocr_text: Optional[str] = None) -> Dict[str, Dict[str, int]]:
    """
    Performs lightweight structured extraction by counting occurrences of label/value pairs.
    """
    corpus = f"{file_text}\n{ocr_text or ''}".lower()
    extraction: Dict[str, Dict[str, int]] = defaultdict(dict)

    # Window/door schedules look like "W2 - 4" etc.
    schedule_pattern = re.compile(r"(w|d)\s?(\d+)\s*[-:]\s*(\d+)")
    for match in schedule_pattern.finditer(corpus):
        symbol = f"{match.group(1).upper()}{match.group(2)}"
        count = int(match.group(3))
        key = "window_schedule" if symbol.startswith("W") else "door_schedule"
        extraction.setdefault(key, {})[symbol] = extraction[key].get(symbol, 0) + count

    # Assemblies / equipment keywords
    for keyword in ["vav", "ahu", "rtu", "panel", "transformer"]:
        occurrences = corpus.count(keyword)
        if occurrences:
            extraction.setdefault("equipment", {})[keyword.upper()] = occurrences

    return extraction


def _match_keywords(text: str, mapping: Dict[str, List[str]]) -> List[str]:
    matches: List[str] = []
    for label, keywords in mapping.items():
        if any(keyword in text for keyword in keywords):
            matches.append(label)
    return matches


def _detect_divisions(text: str) -> List[int]:
    divisions: List[int] = []
    for division, keywords in CSI_DIVISIONS.items():
        if any(keyword in text for keyword in keywords):
            divisions.append(division)
    return divisions

