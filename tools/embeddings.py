from __future__ import annotations

import hashlib
import math
from typing import Any, Dict, List, Optional


def embed_text(text: str, *, client=None, model: str = "text-embedding-3-small") -> Dict[str, Any]:
    """
    Creates (or approximates) an embedding for the supplied text. Attempts to use
    the provided OpenAI client, but falls back to a deterministic hash if the API
    call fails (useful in offline/dev environments).
    """
    normalized = (text or "").strip()
    if not normalized:
        return {"embedding_id": None, "embedding": []}

    embedding: List[float]
    embedding_id: Optional[str] = None

    if client:
        try:
            response = client.embeddings.create(model=model, input=normalized)
            embedding = response.data[0].embedding  # type: ignore[attr-defined]
            embedding_id = getattr(response.data[0], "id", None) or _hash_id(normalized)
            return {"embedding_id": embedding_id, "embedding": embedding}
        except Exception:  # noqa: BLE001
            pass

    # Fallback deterministic vector
    embedding_id = _hash_id(normalized)
    embedding = _fallback_embedding(normalized)
    return {"embedding_id": embedding_id, "embedding": embedding}


def _hash_id(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _fallback_embedding(value: str, dimension: int = 64) -> List[float]:
    """
    Generates a pseudo embedding by hashing the text. Provides deterministic
    vectors when real embeddings are unavailable.
    """
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    nums = [b / 255.0 for b in digest]
    vector = []
    for index in range(dimension):
        vector.append(nums[index % len(nums)] - 0.5)
    norm = math.sqrt(sum(x * x for x in vector)) or 1.0
    return [round(x / norm, 6) for x in vector]

