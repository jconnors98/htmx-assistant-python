"""
Utility functions for the HTMX Assistant application.
This module contains all helper functions that were previously defined in app.py.
"""

import os
import re
import io
import json
import hashlib
import threading
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from bson import ObjectId
from decouple import config
from docx import Document

# Global variables that need to be imported from app.py
_jwks = None


def _get_priority_source(data, default="sites"):
    if not data:
        return default

    preference = data.get("priority_source")
    if isinstance(preference, str):
        preference = preference.strip().lower()
    if preference in {"sites", "files"}:
        return preference

    if data.get("prioritize_files"):
        return "files"

    return default


def _get_jwks():
    global _jwks
    if _jwks is None and config("COGNITO_REGION") and config("COGNITO_USER_POOL_ID"):
        url = (
            f"https://cognito-idp.{config('COGNITO_REGION')}.amazonaws.com/"
            f"{config('COGNITO_USER_POOL_ID')}/.well-known/jwks.json"
        )
        _jwks = requests.get(url, timeout=30).json().get("keys", [])
    return _jwks or []


def _is_super_admin(user_id, superadmins_collection):
    if not user_id:
        return False

    or_clauses = [{"user_id": user_id}]
    try:
        or_clauses.append({"_id": ObjectId(user_id)})
    except Exception:  # noqa: BLE001
        or_clauses.append({"_id": user_id})

    return (
        superadmins_collection.count_documents({"$or": or_clauses}, limit=1) > 0
    )


def _async_log_prompt(prompt=None, response=None, mode=None, ip_addr=None, conversation_id=None, response_id=None, prompt_logs_collection=None):
    ip_hash = hashlib.sha256(ip_addr.encode()).hexdigest() if ip_addr else None
    location = {}
    mode = str(mode) if mode else "<unknown>"
    
    log_type = "response" if response else "prompt"
    print(f"Logging {log_type} from IP: {ip_addr}")
    
    if ip_addr:
        try:
            resp = requests.get(f"https://ipapi.co/{ip_addr}/json/", timeout=30)
            print(f"IP API response: {resp.status_code} {resp.text}")
            if resp.ok:
                data = resp.json()
                location = {
                    "city": data.get("city"),
                    "region": data.get("region"),
                    "country": data.get("country_name"),
                }
        except Exception as e:
            print(f"Error fetching IP info: {e}")
            pass
    else:
        print("No IP address found.")

    log_entry = {
        "mode": mode,
        "conversation_id": conversation_id,
        "ip_hash": ip_hash,
        "location": location,
        "created_at": datetime.utcnow(),
    }

    if prompt:
        log_entry["prompt"] = prompt
    if response:
        log_entry["response"] = response
        log_entry["response_id"] = response_id

    prompt_logs_collection.insert_one(log_entry)


def _parse_date(value, end=False):
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return None
    if end and parsed.time() == datetime.min.time():
        parsed = parsed + timedelta(days=1) - timedelta(microseconds=1)
    return parsed


def _normalize_color(value, default_color="#82002d"):
    if not value:
        return default_color

    text = str(value).strip()
    if not text:
        return default_color

    if not text.startswith("#"):
        text = f"#{text}"

    if not re.fullmatch(r"#[0-9a-fA-F]{6}", text):
        return default_color

    return text.lower()


def _normalize_text_color(value, default_color="#ffffff"):
    if not value:
        return default_color

    text = str(value).strip()
    if not text:
        return default_color

    if not text.startswith("#"):
        text = f"#{text}"

    if not re.fullmatch(r"#[0-9a-fA-F]{6}", text):
        return default_color

    return text.lower()


def _process_natural_language_query(query, pipeline, match, client, prompt_logs_collection, modes_collection):
    """Process natural language queries about analytics data using AI."""
    
    # Get current analytics data for context
    analytics_data = _get_analytics_data_for_query(pipeline, match, prompt_logs_collection, modes_collection)
    
    # Create a prompt for the AI to interpret the query
    system_prompt = """You are an analytics assistant that helps interpret natural language questions about user interaction data.

You have access to these tool functions:
- search_prompts(query_text): Search for prompts containing specific text or keywords
- get_top_prompts(): Get the most frequently used prompt messages and their counts

When answering questions about prompts:
- You can analyze specific prompts and how often they're used via the get_top_prompts tool function
- You can identify the most common questions or requests via the get_top_prompts tool function
- You can find patterns in user behavior based on prompt content via the get_top_prompts tool function
- You can answer questions about specific prompt text or topics via the search_prompts tool function
- You can search for prompts containing certain keywords or phrases via the search_prompts tool function

When answering questions:
1. Interpret what the user is asking for
2. Use the appropriate tool functions to gather specific data
3. Provide a clear, helpful answer based on the data
4. Include specific numbers and insights
5. If the question can't be answered confidently and in full with the available data, use the appropriate tool functions to gather specific data

Provide your response as a clear, conversational, human-readable answer. Include specific numbers, insights, and data points directly in your response. Be helpful and informative."""

    user_prompt = f"""Analytics Data:
{analytics_data}

User Question: "{query}"

Please analyze this question and provide insights based on the available data."""

    try:
        # Define the available tools for the AI (Responses API format)
        tools = [
            {
                "type": "function",
                "name": "search_prompts",
                "description": "Search for prompts containing specific text or keywords",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query_text": {
                            "type": "string",
                            "description": "The text or keywords to search for in prompts"
                        }
                    },
                    "required": ["query_text"]
                }
            },
            {
                "type": "function", 
                "name": "get_top_prompts",
                "description": "Get the most frequently used prompts",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of prompts to return (default: 20)"
                        }
                    },
                    "required": []
                }
            }
        ]
        
        # Use Responses API instead of Chat Completions
        response = client.responses.create(
            model="gpt-4o-mini",
            tools=tools,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        
        # Handle function calls if the AI wants to use tools
        if hasattr(response, 'output') and response.output and len(response.output) > 0:
            output = response.output[0]
            print("output", output)
            # Check for tool calls directly on the output
            if output.type == "function_call":

                print("tool call: ", output)
                function_name = output.name
                function_args = output.arguments
                
                # Parse function arguments
                if isinstance(function_args, str):
                    function_args = json.loads(function_args)
                
                # Execute the requested tool function
                if function_name == "search_prompts":
                    search_results = _search_prompts_tool(
                        function_args.get("query_text", ""), 
                        pipeline, 
                        match,
                        prompt_logs_collection
                    )
                    tool_result = f"Found {len(search_results)} matching prompts:\n" + "\n".join([f"- \"{p['prompt']}\" (used {p['count']} times)" for p in search_results])
                    
                elif function_name == "get_top_prompts":
                    limit = function_args.get("limit", 20)
                    top_prompts = _get_unique_prompts_data(pipeline, match, prompt_logs_collection, limit)
                    tool_result = f"Top {len(top_prompts)} most frequent prompts:\n" + "\n".join([f"- \"{p['prompt']}\" (used {p['count']} times)" for p in top_prompts])
                else:
                    tool_result = "Unknown tool function requested."
                
                # Get the AI's final response with tool results
                final_response = client.responses.create(
                    model="gpt-4o-mini",
                    input=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                        output,
                        {"output": tool_result, "call_id": output.call_id, "type": "function_call_output"},
                    ],
                )
                return {"answer": final_response.output_text}
        
        return {"answer": response.output_text}
        
    except Exception as e:
        print(f"Error calling AI for query processing: {e}")
        return {
            "error": "Unable to process your question with AI. Please try a simpler question."
        }


def _search_prompts_tool(query_text, pipeline, match, prompt_logs_collection, limit=20):
    """Tool function to search for prompts containing specific text or patterns."""
    
    # Create a regex pattern for case-insensitive search
    search_pattern = re.escape(query_text.lower())
    
    # Search for prompts containing the query text
    matching_prompts = []
    for doc in prompt_logs_collection.aggregate(
        pipeline + [
            {
                "$match": {
                    **match, 
                    "prompt": {
                        "$exists": True, 
                        "$ne": "",
                        "$regex": search_pattern,
                        "$options": "i"
                    }
                }
            },
            {"$group": {"_id": "$prompt", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit},
        ]
    ):
        prompt_text = doc.get("_id", "")
        count = doc.get("count", 0)
        
        # Truncate very long prompts for readability
        display_text = prompt_text[:200] + "..." if len(prompt_text) > 200 else prompt_text
        
        matching_prompts.append({
            "prompt": display_text,
            "full_prompt": prompt_text,
            "count": count
        })
    
    return matching_prompts


def _get_unique_prompts_data(pipeline, match, prompt_logs_collection, limit=50):
    """Get unique prompts and their repetition counts for AI analysis."""
    
    # Get unique prompts with their counts
    unique_prompts = []
    for doc in prompt_logs_collection.aggregate(
        pipeline + [
            {"$match": {**match, "prompt": {"$exists": True, "$ne": ""}}},
            {"$group": {"_id": "$prompt", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit},
        ]
    ):
        prompt_text = doc.get("_id", "")
        count = doc.get("count", 0)
        
        # Truncate very long prompts for readability
        display_text = prompt_text[:200] + "..." if len(prompt_text) > 200 else prompt_text
        
        unique_prompts.append({
            "prompt": display_text,
            "full_prompt": prompt_text,
            "count": count
        })
    print("unique_prompts", unique_prompts)
    return unique_prompts


def _search_permits_tool(query_text, limit=10):
    """Tool function to search for permits in MySQL database using project_description column."""
    print("searching permits", query_text, limit)
    try:
        import mysql.connector
        from decouple import config
        
        # Use the same connection details from mysql_test.py
        cnx = mysql.connector.connect(
            host=config("MYSQL_HOST"),
            database=config("MYSQL_DATABASE"),
            port=3306,
            user=config("MYSQL_USER"),
            password=config("MYSQL_PASSWORD"),
            ssl_ca=config("MYSQL_CERT_PATH"),
            collation="utf8mb4_unicode_ci"
        )
        print("connected to mysql")
        cursor = cnx.cursor(dictionary=True)
        
        # Search for permits using LIKE with wildcards for partial matches
        search_query = """
            SELECT id, project_description, status, source, 
                   date_added, permit_number
            FROM permit_data 
            WHERE project_description LIKE %s 
            ORDER BY date_added DESC 
            LIMIT %s
        """
        print("search query", search_query)
        
        # Add wildcards for partial matching
        search_pattern = f"%{query_text}%"
        cursor.execute(search_query, (search_pattern, limit))
        
        results = cursor.fetchall()
        print("results", results)
        
        cursor.close()
        cnx.close()
        
        return results
        
    except Exception as e:
        print(f"Error searching permits: {e}")
        return []


def _search_jobs_tool(query_text, *, user_id, limit=10, use_profile=True):
    """
    Tool function to search for jobs in MySQL, optionally using the user's profile.

    - Searches across title/description/requirements via LIKE patterns.
    - If use_profile is True, loads the user's resume from users.resume_path and extracts keywords.
    - Uses a soft location preference (cities.id in users.location) to rank results.
    """

    def _safe_int(value):
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return None

    def _is_url(value):
        if not value:
            return False
        text = str(value).strip().lower()
        return text.startswith("http://") or text.startswith("https://")

    def _extract_keywords(text, *, max_terms=12):
        if not text:
            return []

        # Basic tokenization + stopword filtering. Keep simple and dependency-free.
        stopwords = {
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "have",
            "in", "is", "it", "its", "of", "on", "or", "our", "that", "the", "their", "this",
            "to", "was", "were", "will", "with", "you", "your", "i", "we", "they", "he", "she",
            "them", "us", "me", "my", "mine", "his", "her", "hers", "who", "what", "when",
            "where", "why", "how",
        }
        tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9\-\+\.]{1,}", str(text).lower())
        freq = {}
        for tok in tokens:
            if len(tok) < 3:
                continue
            if tok in stopwords:
                continue
            # Skip ultra-common resume noise
            if tok in {"resume", "curriculum", "vitae", "experience", "skills", "education"}:
                continue
            freq[tok] = freq.get(tok, 0) + 1

        ranked = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
        return [t for t, _ in ranked[:max_terms]]

    def _load_resume_text(resume_path):
        if not resume_path:
            return ""
        path_value = str(resume_path).strip()
        if not path_value:
            return ""

        try:
            import tempfile
            import os

            local_path = None
            if _is_url(path_value):
                # Download to a temp file (best-effort).
                url = path_value
                suffix = os.path.splitext(url.split("?")[0])[1].lower()
                if suffix not in {".pdf", ".txt", ".doc", ".docx"}:
                    suffix = ".bin"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    resp = requests.get(url, timeout=30)
                    resp.raise_for_status()
                    tmp.write(resp.content)
                    local_path = tmp.name
            else:
                local_path = path_value

            if not local_path or not os.path.exists(local_path):
                print("resume not found", local_path)
                return ""

            ext = os.path.splitext(local_path)[1].lower()
            if ext == ".pdf":
                try:
                    from tools import pdf_tools

                    parsed = pdf_tools.parse_pdf(local_path, max_seconds=30)
                    return (parsed or {}).get("text", "") or ""
                except Exception as e:  # noqa: BLE001
                    print(f"Resume PDF parse failed: {e}")
                    return ""

            if ext == ".docx":
                # Prefer python-docx when available, then fall back to raw XML parsing.
                try:
                    doc = Document(local_path)
                    text = "\n".join(p.text for p in doc.paragraphs if p.text)
                    if text.strip():
                        return text
                except Exception:
                    pass

                try:
                    import zipfile
                    from xml.etree import ElementTree as ET

                    with zipfile.ZipFile(local_path) as zf:
                        doc_xml = zf.read("word/document.xml")

                    root = ET.fromstring(doc_xml)
                    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                    paragraphs = []
                    for para in root.findall(".//w:p", ns):
                        parts = [node.text for node in para.findall(".//w:t", ns) if node.text]
                        if parts:
                            paragraphs.append("".join(parts))
                    return "\n".join(paragraphs)
                except Exception as e:  # noqa: BLE001
                    print(f"Resume DOCX parse failed: {e}")
                    return ""

            if ext == ".doc":
                # Legacy Word format: try native extractors first, then LibreOffice conversion.
                try:
                    import shutil
                    import subprocess

                    for extractor in ("antiword", "catdoc"):
                        if not shutil.which(extractor):
                            continue
                        try:
                            proc = subprocess.run(
                                [extractor, local_path],
                                capture_output=True,
                                timeout=30,
                                check=False,
                            )
                            parsed_text = (proc.stdout or b"").decode("utf-8", errors="ignore").strip()
                            if parsed_text:
                                return parsed_text
                        except Exception as e:  # noqa: BLE001
                            print(f"{extractor} parse failed: {e}")

                    soffice_bin = shutil.which("soffice") or shutil.which("libreoffice")
                    if soffice_bin:
                        try:
                            with tempfile.TemporaryDirectory() as out_dir:
                                subprocess.run(
                                    [
                                        soffice_bin,
                                        "--headless",
                                        "--convert-to",
                                        "txt:Text",
                                        "--outdir",
                                        out_dir,
                                        local_path,
                                    ],
                                    capture_output=True,
                                    timeout=45,
                                    check=False,
                                )
                                converted = os.path.join(
                                    out_dir,
                                    f"{os.path.splitext(os.path.basename(local_path))[0]}.txt",
                                )
                                if os.path.exists(converted):
                                    with open(converted, "rb") as handle:
                                        raw = handle.read()
                                    parsed_text = raw.decode("utf-8", errors="ignore").strip()
                                    if parsed_text:
                                        return parsed_text
                        except Exception as e:  # noqa: BLE001
                            print(f"soffice doc conversion failed: {e}")
                except Exception as e:  # noqa: BLE001
                    print(f"Resume DOC parse failed: {e}")
                return ""

            # Best-effort plaintext read for non-PDF files.
            with open(local_path, "rb") as handle:
                raw = handle.read()
            try:
                return raw.decode("utf-8")
            except Exception:  # noqa: BLE001
                return raw.decode("utf-8", errors="ignore")

        except Exception as e:  # noqa: BLE001
            print(f"Resume load failed: {e}")
            return ""

    def _geocode_place(place, _cache={}):  # noqa: B006
        """
        Geocode a 'City, ProvinceCode' style string to (lat, lon).
        Uses OpenStreetMap Nominatim (best-effort) with a tiny in-process cache.
        """
        if not place:
            return None
        key = str(place).strip().lower()
        if not key:
            return None
        if key in _cache:
            return _cache[key]

        try:
            resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": place, "format": "json", "limit": 1},
                headers={"User-Agent": "htmx-assistant-python/1.0"},
                timeout=15,
            )
            if not resp.ok:
                _cache[key] = None
                return None
            data = resp.json() or []
            if not data:
                _cache[key] = None
                return None
            lat = float(data[0].get("lat"))
            lon = float(data[0].get("lon"))
            _cache[key] = (lat, lon)
            return _cache[key]
        except Exception as e:  # noqa: BLE001
            print(f"Geocode failed for '{place}': {e}")
            _cache[key] = None
            return None

    def _haversine_km(a, b):
        import math

        (lat1, lon1) = a
        (lat2, lon2) = b
        r = 6371.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        x = (
            math.sin(dphi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        )
        return 2 * r * math.atan2(math.sqrt(x), math.sqrt(1 - x))

    print("searching jobs", query_text, user_id, limit, use_profile)
    cnx = None
    cursor = None
    try:
        import mysql.connector
        from decouple import config

        # Sane limit
        try:
            limit = int(limit)
        except Exception:  # noqa: BLE001
            limit = 10
        limit = max(1, min(limit, 25))

        user_id_int = _safe_int(user_id)

        # Use the same connection details from mysql_test.py / permits tool
        cnx = mysql.connector.connect(
            host=config("MYSQL_HOST"),
            database=config("MYSQL_JOBS_DATABASE"),
            port=3306,
            user=config("MYSQL_USER"),
            password=config("MYSQL_PASSWORD"),
            # ssl_ca=config("MYSQL_CERT_PATH"),
            collation="utf8mb4_unicode_ci",
        )
        cursor = cnx.cursor(dictionary=True)

        user_location = None
        user_city_id = None
        user_location_text_legacy = None
        commute_radius_km = None
        resume_path = None
        if user_id_int is not None:
            cursor.execute(
                "SELECT id, location, commute_radius, resume_path FROM users WHERE id=%s LIMIT 1",
                (user_id_int,),
            )
            user_row = cursor.fetchone() or {}
            print("user_row", user_row)
            # New data model: users.location stores a cities.id (int).
            user_city_id = _safe_int(user_row.get("location"))
            if user_city_id is None:
                # Backward compatible with legacy "City, ProvinceCode" strings during migration.
                user_location_text_legacy = str(user_row.get("location") or "").strip() or None
            user_location = user_city_id if user_city_id is not None else user_location_text_legacy
            try:
                commute_radius_km = int(user_row.get("commute_radius") or 0)
            except Exception:  # noqa: BLE001
                commute_radius_km = None
            resume_path = (user_row.get("resume_path") or "").strip() or None

        # Build term list from query + resume keywords
        terms = []
        if query_text and str(query_text).strip():
            qt = str(query_text).strip()
            # Include the full phrase (trimmed) plus token keywords
            if len(qt) <= 80:
                terms.append(qt.lower())
            terms.extend(_extract_keywords(qt, max_terms=8))

        if use_profile and resume_path:
            resume_text = _load_resume_text(resume_path)
            terms.extend(_extract_keywords(resume_text, max_terms=12))

        # Dedupe while preserving order; keep query-derived terms first.
        seen = set()
        deduped_terms = []
        for t in terms:
            if not t:
                continue
            key = str(t).strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped_terms.append(key)

        # Avoid massive SQL parameter lists
        deduped_terms = deduped_terms[:8]

        params = []
        # We'll compute location_score using commute_radius in Python post-processing.
        location_score_sql = "0 AS location_score"

        if deduped_terms:
            relevance_parts = []
            for term in deduped_terms:
                pattern = f"%{term}%"
                relevance_parts.append("(CASE WHEN jobs.title LIKE %s THEN 3 ELSE 0 END)")
                relevance_parts.append("(CASE WHEN jobs.description LIKE %s THEN 1 ELSE 0 END)")
                relevance_parts.append("(CASE WHEN jobs.requirements LIKE %s THEN 1 ELSE 0 END)")
                params.extend([pattern, pattern, pattern])
            relevance_score_sql = f"({'+'.join(relevance_parts)}) AS relevance_score"
        else:
            relevance_score_sql = "0 AS relevance_score"

        where_sql = "WHERE status='active' AND (expires_at IS NULL OR expires_at > NOW())"
        if deduped_terms:
            term_groups = []
            term_params = []
            for term in deduped_terms:
                pattern = f"%{term}%"
                term_groups.append(
                    "(jobs.title LIKE %s OR jobs.description LIKE %s OR jobs.requirements LIKE %s)"
                )
                term_params.extend([pattern, pattern, pattern])
            where_sql += " AND (" + " OR ".join(term_groups) + ")"
            params.extend(term_params)

        search_query = f"""
            SELECT
                jobs.id,
                jobs.employer_id,
                jobs.title,
                jobs.description,
                jobs.requirements,
                jobs.salary_min,
                jobs.salary_max,
                jobs.salary_type,
                jobs.job_type,
                jobs.experience_level,
                jobs.is_apprenticeship,
                jobs.pre_interview_enabled,
                jobs.status,
                jobs.views_count,
                jobs.created_at,
                jobs.updated_at,
                jobs.expires_at,
                jobs.location AS city_id,
                COALESCE(CONCAT(cities.name, ', ', cities.province_code), CAST(jobs.location AS CHAR)) AS location,
                cities.latitude AS city_latitude,
                cities.longitude AS city_longitude,
                jobs.application_method,
                jobs.external_url,
                {location_score_sql},
                {relevance_score_sql}
            FROM jobs
            LEFT JOIN cities ON cities.id = jobs.location
            {where_sql}
            ORDER BY location_score DESC, relevance_score DESC, created_at DESC
            LIMIT %s
        """

        params.append(limit)
        cursor.execute(search_query, tuple(params))
        results = cursor.fetchall()

        # Post-process location scoring using commute radius (km).
        # Prefer cities table coordinates; fall back to geocoding only if needed.
        if results and (user_city_id is not None or user_location_text_legacy):
            try:
                # Default to schema default if unset/invalid.
                if commute_radius_km is None or commute_radius_km <= 0:
                    commute_radius_km = 30

                user_geo = None
                user_location_display = None

                if user_city_id is not None:
                    cursor.execute(
                        "SELECT name, province_code, latitude, longitude FROM cities WHERE id=%s LIMIT 1",
                        (user_city_id,),
                    )
                    user_city = cursor.fetchone() or {}
                    u_name = user_city.get("name")
                    u_prov = user_city.get("province_code")
                    u_lat = user_city.get("latitude")
                    u_lon = user_city.get("longitude")
                    if u_name and u_prov:
                        user_location_display = f"{u_name}, {u_prov}"
                    if u_lat is not None and u_lon is not None:
                        user_geo = (float(u_lat), float(u_lon))

                # Final fallback: legacy string location geocoding (should disappear after migration).
                if not user_geo and user_location_text_legacy:
                    user_geo = _geocode_place(user_location_text_legacy)

                user_loc_lc = (
                    str(user_location_display or user_location_text_legacy or "")
                    .strip()
                    .lower()
                )

                for job in results:
                    job_loc = (job.get("location") or "").strip()
                    job["distance_km"] = None
                    job["location_score"] = 0

                    if not job_loc:
                        continue

                    # Exact same city => perfect match even if coords are missing.
                    job_city_id = _safe_int(job.get("city_id"))
                    if user_city_id is not None and job_city_id is not None and job_city_id == user_city_id:
                        job["distance_km"] = 0.0
                        job["location_score"] = 1
                        continue

                    if user_geo:
                        lat = job.get("city_latitude")
                        lon = job.get("city_longitude")
                        job_geo = None
                        if lat is not None and lon is not None:
                            try:
                                job_geo = (float(lat), float(lon))
                            except Exception:  # noqa: BLE001
                                job_geo = None

                        # Fallback: only geocode if we don't have city coordinates
                        if not job_geo:
                            job_geo = _geocode_place(job_loc)

                        if job_geo:
                            dist = _haversine_km(user_geo, job_geo)
                            job["distance_km"] = round(dist, 1)
                            if dist <= commute_radius_km:
                                job["location_score"] = 1
                                continue

                    # Fallback: simple city/province substring check
                    if user_loc_lc and user_loc_lc in job_loc.lower():
                        job["location_score"] = 1

                # Re-sort by location_score then relevance_score then created_at.
                def _sort_key(job):
                    created = job.get("created_at")
                    created_val = created.timestamp() if hasattr(created, "timestamp") else 0
                    return (
                        int(job.get("location_score") or 0),
                        float(job.get("relevance_score") or 0),
                        created_val,
                    )

                results.sort(key=_sort_key, reverse=True)
            except Exception as e:  # noqa: BLE001
                print(f"Location scoring failed: {e}")

        try:
            if cursor:
                cursor.close()
        finally:
            if cnx:
                cnx.close()

        return results

    except Exception as e:  # noqa: BLE001
        print(f"Error searching jobs: {e}")
        try:
            if cursor:
                cursor.close()
        finally:
            if cnx:
                cnx.close()
        return []


def _get_analytics_data_for_query(pipeline, match, prompt_logs_collection, modes_collection):
    """Get relevant analytics data for AI processing."""
    
    # Create a filter for user prompts only (excludes AI responses)
    prompt_match = {**match, "prompt": {"$exists": True}}
    
    # Get basic counts
    total_prompts = prompt_logs_collection.count_documents(prompt_match)
    total_responses = prompt_logs_collection.count_documents({**match, "response": {"$exists": True}})
    unique_conversations = len([cid for cid in prompt_logs_collection.distinct("conversation_id", prompt_match) if cid])
    unique_users = len([ip for ip in prompt_logs_collection.distinct("ip_hash", prompt_match) if ip])
    
    # Get top modes
    mode_counts = [
        {"mode_id": doc.get("_id"), "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline + [
                {"$group": {"_id": "$mode", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
            ]
        )
    ]
    
    top_modes_text = []
    for mode_data in mode_counts:
        mode_id = mode_data["mode_id"]
        if mode_id:
            try:
                mode_doc = modes_collection.find_one({"_id": ObjectId(mode_id)})
                mode_title = mode_doc.get("title") or mode_doc.get("name") if mode_doc else "Unknown"
            except Exception:
                mode_title = "Unknown"
        else:
            mode_title = "Unknown"
        top_modes_text.append(f"- {mode_title}: {mode_data['count']} interactions")
    
    # Get top countries
    top_countries = [
        {"country": doc.get("_id") or "Unknown", "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline + [
                {
                    "$group": {
                        "_id": {"$ifNull": ["$location.country", "Unknown"]},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 5},
            ]
        )
    ]
    
    top_countries_text = [f"- {country['country']}: {country['count']} interactions" for country in top_countries]
    
    # Get daily activity (last 7 days)
    daily_counts = [
        {"date": doc.get("_id"), "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline + [
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$created_at",
                            }
                        },
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": -1}},
                {"$limit": 7},
            ]
        )
    ]
    
    daily_text = [f"- {day['date']}: {day['count']} interactions" for day in daily_counts]
    
    return f"""Summary Statistics:
- Total Prompts: {total_prompts}
- Total Responses: {total_responses}
- Unique Conversations: {unique_conversations}
- Unique Users: {unique_users}

Top Modes by Usage:
{chr(10).join(top_modes_text)}

Top Countries by Usage:
{chr(10).join(top_countries_text)}

Recent Daily Activity (last 7 days):
{chr(10).join(daily_text)}"""
