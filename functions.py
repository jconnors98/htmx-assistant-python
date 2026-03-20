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
from pymongo import MongoClient
from pymongo.server_api import ServerApi

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


_RESUME_CONTEXT_CACHE: Dict[str, Dict[str, Any]] = {}
_RESUME_CONTEXT_CACHE_LOCK = threading.Lock()
_RESUME_CONTEXT_CACHE_TTL_SECONDS = 900


def _safe_int_value(value):
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return None


def _is_url_path(value):
    if not value:
        return False
    text = str(value).strip().lower()
    return text.startswith("http://") or text.startswith("https://")


def _extract_resume_keywords(text, *, max_terms=12):
    if not text:
        return []

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
        if tok in {"resume", "curriculum", "vitae", "experience", "skills", "education"}:
            continue
        freq[tok] = freq.get(tok, 0) + 1

    ranked = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
    return [t for t, _ in ranked[:max_terms]]


def _connect_jobs_db():
    mongo_client = MongoClient(config("MONGO_URI"), server_api=ServerApi("1"))
    db_name = config("MONGO_DB_TC", default="talentcentral")
    mongo_db = mongo_client.get_database(db_name)
    return mongo_client, {
        "users": mongo_db.get_collection("users"),
        "jobs": mongo_db.get_collection("jobs"),
        "locations": mongo_db.get_collection("locations"),
    }


def _load_resume_text_from_path(resume_path):
    if not resume_path:
        return ""
    path_value = str(resume_path).strip()
    if not path_value:
        return ""

    try:
        import tempfile

        local_path = None
        if _is_url_path(path_value):
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
                import tempfile

                try:
                    file_size = os.path.getsize(local_path)
                except Exception as file_size_error:  # noqa: BLE001
                    file_size = None
                    print(f"[resume-doc] Could not read file size for {local_path}: {file_size_error}")
                print(
                    f"[resume-doc] Begin parsing DOC file: path={local_path} "
                    f"exists={os.path.exists(local_path)} size_bytes={file_size}"
                )

                empty_reasons = []
                for extractor in ("antiword", "catdoc"):
                    extractor_path = shutil.which(extractor)
                    if not extractor_path:
                        reason = f"{extractor} not installed"
                        empty_reasons.append(reason)
                        print(f"[resume-doc] Skipping extractor: {reason}")
                        continue
                    print(f"[resume-doc] Trying extractor={extractor} bin={extractor_path}")
                    try:
                        proc = subprocess.run(
                            [extractor, local_path],
                            capture_output=True,
                            timeout=30,
                            check=False,
                        )
                        stdout_bytes = proc.stdout or b""
                        stderr_bytes = proc.stderr or b""
                        print(
                            f"[resume-doc] Extractor={extractor} return_code={proc.returncode} "
                            f"stdout_bytes={len(stdout_bytes)} stderr_bytes={len(stderr_bytes)}"
                        )
                        parsed_text = (proc.stdout or b"").decode("utf-8", errors="ignore").strip()
                        if parsed_text:
                            preview = parsed_text[:200].replace("\n", "\\n")
                            print(
                                f"[resume-doc] Extractor={extractor} succeeded "
                                f"text_chars={len(parsed_text)} preview={preview!r}"
                            )
                            return parsed_text
                        stderr_preview = stderr_bytes.decode("utf-8", errors="ignore").strip()[:200]
                        reason = (
                            f"{extractor} produced empty text "
                            f"(return_code={proc.returncode}, stderr_preview={stderr_preview!r})"
                        )
                        empty_reasons.append(reason)
                        print(f"[resume-doc] {reason}")
                    except Exception as e:  # noqa: BLE001
                        empty_reasons.append(f"{extractor} raised exception: {e}")
                        print(f"{extractor} parse failed: {e}")

                soffice_bin = shutil.which("soffice") or shutil.which("libreoffice")
                if soffice_bin:
                    print(f"[resume-doc] Trying LibreOffice conversion with bin={soffice_bin}")
                    try:
                        with tempfile.TemporaryDirectory() as out_dir:
                            convert_proc = subprocess.run(
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
                            convert_stdout = (convert_proc.stdout or b"").decode("utf-8", errors="ignore").strip()
                            convert_stderr = (convert_proc.stderr or b"").decode("utf-8", errors="ignore").strip()
                            print(
                                f"[resume-doc] LibreOffice conversion return_code={convert_proc.returncode} "
                                f"stdout_preview={convert_stdout[:200]!r} stderr_preview={convert_stderr[:200]!r}"
                            )
                            converted = os.path.join(
                                out_dir,
                                f"{os.path.splitext(os.path.basename(local_path))[0]}.txt",
                            )
                            print(
                                f"[resume-doc] Converted TXT path={converted} "
                                f"exists={os.path.exists(converted)}"
                            )
                            if os.path.exists(converted):
                                with open(converted, "rb") as handle:
                                    raw = handle.read()
                                parsed_text = raw.decode("utf-8", errors="ignore").strip()
                                if parsed_text:
                                    preview = parsed_text[:200].replace("\n", "\\n")
                                    print(
                                        f"[resume-doc] LibreOffice conversion succeeded "
                                        f"text_chars={len(parsed_text)} preview={preview!r}"
                                    )
                                    return parsed_text
                                reason = "LibreOffice produced TXT file but extracted text is empty"
                                empty_reasons.append(reason)
                                print(f"[resume-doc] {reason}")
                            else:
                                reason = "LibreOffice did not produce converted TXT output file"
                                empty_reasons.append(reason)
                                print(f"[resume-doc] {reason}")
                    except Exception as e:  # noqa: BLE001
                        empty_reasons.append(f"LibreOffice conversion exception: {e}")
                        print(f"soffice doc conversion failed: {e}")
                else:
                    reason = "Neither soffice nor libreoffice executable was found"
                    empty_reasons.append(reason)
                    print(f"[resume-doc] {reason}")
            except Exception as e:  # noqa: BLE001
                print(f"Resume DOC parse failed: {e}")
            if empty_reasons:
                print(f"[resume-doc] Returning empty text for DOC. Reasons: {empty_reasons}")
            else:
                print("[resume-doc] Returning empty text for DOC with no detailed reasons captured.")
            return ""

        with open(local_path, "rb") as handle:
            raw = handle.read()
        try:
            return raw.decode("utf-8")
        except Exception:  # noqa: BLE001
            return raw.decode("utf-8", errors="ignore")

    except Exception as e:  # noqa: BLE001
        print(f"Resume load failed: {e}")
        return ""


def _get_talentcentral_user_profile(collections, user_id):
    user_id_int = _safe_int_value(user_id)
    user_id_text = str(user_id).strip()
    profile = {
        "effective_user_id": user_id_text,
        "user_id_int": user_id_int,
        "user_profile_found": False,
        "user_city_id": None,
        "user_location_text_legacy": None,
        "user_location": None,
        "commute_radius_km": None,
        "resume_path": None,
    }
    users_collection = collections["users"]

    or_filters = []
    if user_id_text:
        or_filters.append({"_id": user_id_text})
    if user_id_int is not None:
        or_filters.append({"legacyUserId": user_id_int})

    user_doc = users_collection.find_one({"$or": or_filters}) if or_filters else None
    profile["user_profile_found"] = bool(user_doc)
    if not user_doc:
        return profile

    jobseeker_profile = user_doc.get("jobseekerProfile") or {}
    location_data = jobseeker_profile.get("location") or {}

    profile["user_city_id"] = _safe_int_value(location_data.get("cityId"))
    if profile["user_city_id"] is None:
        city_name = str(location_data.get("city") or "").strip()
        province_code = str(location_data.get("provinceCode") or "").strip()
        if city_name and province_code:
            profile["user_location_text_legacy"] = f"{city_name}, {province_code}"
        elif city_name:
            profile["user_location_text_legacy"] = city_name
    profile["user_location"] = profile["user_city_id"] if profile["user_city_id"] is not None else profile["user_location_text_legacy"]
    try:
        profile["commute_radius_km"] = int(jobseeker_profile.get("commuteRadius") or 0)
    except Exception:  # noqa: BLE001
        profile["commute_radius_km"] = None
    profile["resume_path"] = (jobseeker_profile.get("resumePath") or "").strip() or None
    return profile


def _get_cached_resume_payload(*, user_id, resume_path):
    key = f"{user_id}:{resume_path}"
    now = datetime.utcnow()

    with _RESUME_CONTEXT_CACHE_LOCK:
        cached = _RESUME_CONTEXT_CACHE.get(key)
        if cached:
            age_seconds = (now - cached["created_at"]).total_seconds()
            if age_seconds <= _RESUME_CONTEXT_CACHE_TTL_SECONDS:
                payload = dict(cached)
                payload["cache_hit"] = True
                return payload
            _RESUME_CONTEXT_CACHE.pop(key, None)

    resume_text = _load_resume_text_from_path(resume_path)
    payload = {
        "created_at": now,
        "resume_text": resume_text,
        "top_skills_keywords": _extract_resume_keywords(resume_text, max_terms=16),
        "experience_signals": _extract_resume_keywords(resume_text, max_terms=8),
        "cache_hit": False,
    }
    with _RESUME_CONTEXT_CACHE_LOCK:
        _RESUME_CONTEXT_CACHE[key] = payload
    return dict(payload)


def _build_resume_summary(resume_text, *, max_chars=1200):
    normalized = " ".join(str(resume_text or "").split())
    if not normalized:
        return ""
    return normalized[:max_chars].strip()


def _get_resume_context_tool(*, user_id, max_chars=1200):
    """
    Return concise resume context for signed-in TalentCentral users.
    This is intentionally compact to keep tool tokens and latency low.
    """
    print("getting resume context", user_id, max_chars)
    mongo_client = None
    collections = None
    try:
        mongo_client, collections = _connect_jobs_db()
        profile = _get_talentcentral_user_profile(collections, user_id)
        resume_path = profile.get("resume_path")
        print("resume path", resume_path)
        if not resume_path:
            return {
                "has_resume": False,
                "has_profile": bool(profile.get("user_profile_found")),
                "resume_summary": "",
                "top_skills_keywords": [],
                "experience_signals": [],
                "max_chars": int(max_chars),
                "cache_hit": False,
            }

        resume_payload = _get_cached_resume_payload(
            user_id=profile.get("user_id_int") or str(user_id),
            resume_path=resume_path,
        )
        print("resume payload", resume_payload)
        resume_text = resume_payload.get("resume_text") or ""
        summary = _build_resume_summary(resume_text, max_chars=max(300, min(int(max_chars), 2200)))

        return {
            "has_resume": bool(resume_text.strip()),
            "has_profile": bool(profile.get("user_profile_found")),
            "resume_summary": summary,
            "top_skills_keywords": (resume_payload.get("top_skills_keywords") or [])[:12],
            "experience_signals": (resume_payload.get("experience_signals") or [])[:6],
            "max_chars": int(max_chars),
            "cache_hit": bool(resume_payload.get("cache_hit")),
            "resume_path_present": bool(resume_path),
        }
    except Exception as e:  # noqa: BLE001
        print(f"Resume context tool failed: {e}")
        return {
            "has_resume": False,
            "has_profile": False,
            "resume_summary": "",
            "top_skills_keywords": [],
            "experience_signals": [],
            "max_chars": int(max_chars),
            "cache_hit": False,
            "error": "resume_context_unavailable",
        }
    finally:
        if mongo_client:
            mongo_client.close()


def _search_jobs_tool(query_text, *, user_id, limit=10, use_profile=True):
    """
    Tool function to search for jobs in MongoDB, optionally using the user's profile.
    """
    print("searching jobs", query_text, user_id, limit, use_profile)

    def _safe_int(value):
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return None

    def _extract_keywords(text, *, max_terms=12):
        return _extract_resume_keywords(text, max_terms=max_terms)

    def _is_vague_job_query(text):
        if text is None:
            return True
        normalized = " ".join(str(text).strip().lower().split())
        if not normalized:
            return True
        tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9\-\+\.]{0,}", normalized)
        if not tokens:
            return True
        generic_tokens = {
            "find", "me", "a", "an", "the", "job", "jobs", "work", "role", "roles",
            "help", "can", "could", "you", "please", "for", "to", "with", "my",
            "resume", "based", "on", "looking", "look",
        }
        meaningful = [tok for tok in tokens if tok not in generic_tokens]
        return len(meaningful) == 0

    def _resolve_city_ids_from_query(locations_collection, text):
        if not text:
            return []
        query_text_norm = " ".join(str(text).strip().split()).lower()
        if not query_text_norm:
            return []

        city_ids = []
        seen = set()
        try:
            rows = locations_collection.find({}, {"legacyCityId": 1, "city": 1, "provinceCode": 1})
            for row in rows:
                city_name = str((row or {}).get("city") or "").strip().lower()
                province_code = str((row or {}).get("provinceCode") or "").strip().lower()
                city_id = _safe_int((row or {}).get("legacyCityId"))
                if not city_name or city_id is None:
                    continue
                if city_name in query_text_norm or f"{city_name}, {province_code}" == query_text_norm:
                    if city_id not in seen:
                        seen.add(city_id)
                        city_ids.append(city_id)
                    if len(city_ids) >= 10:
                        break
        except Exception as e:  # noqa: BLE001
            print(f"City resolution from query failed: {e}")
            return []

        print("jobs city resolution", {"query": query_text_norm, "matched_city_ids": city_ids, "match_count": len(city_ids)})
        return city_ids

    def _geocode_place(place, _cache={}):  # noqa: B006
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
        x = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        return 2 * r * math.atan2(math.sqrt(x), math.sqrt(1 - x))

    def _normalize_job(job_doc):
        salary = job_doc.get("salary") or {}
        location = job_doc.get("location") or {}
        city_name = str(location.get("city") or "").strip()
        province_code = str(location.get("provinceCode") or "").strip()
        if city_name and province_code:
            location_text = f"{city_name}, {province_code}"
        else:
            location_text = city_name or str(location.get("cityId") or "").strip()
        return {
            "id": str(job_doc.get("legacyJobId") or job_doc.get("_id") or ""),
            "employer_id": job_doc.get("legacyEmployerId"),
            "title": job_doc.get("title"),
            "description": job_doc.get("description"),
            "requirements": job_doc.get("requirements"),
            "salary_min": salary.get("min"),
            "salary_max": salary.get("max"),
            "salary_type": salary.get("type"),
            "job_type": job_doc.get("jobType"),
            "experience_level": job_doc.get("experienceLevel"),
            "is_apprenticeship": bool(job_doc.get("isApprenticeship")),
            "pre_interview_enabled": bool(job_doc.get("preInterviewEnabled")),
            "status": job_doc.get("status"),
            "views_count": job_doc.get("viewsCount", 0),
            "created_at": job_doc.get("createdAt"),
            "updated_at": job_doc.get("updatedAt"),
            "expires_at": job_doc.get("expiresAt"),
            "city_id": _safe_int(location.get("cityId")),
            "location": location_text,
            "city_latitude": location.get("latitude"),
            "city_longitude": location.get("longitude"),
            "application_method": job_doc.get("applicationMethod"),
            "external_url": job_doc.get("externalUrl"),
            "location_score": 0,
            "relevance_score": 0,
        }

    def _calc_relevance(job_doc, terms):
        title_text = str(job_doc.get("title") or "").lower()
        description_text = str(job_doc.get("description") or "").lower()
        requirements_text = str(job_doc.get("requirements") or "").lower()
        score = 0
        for term in terms:
            if term in title_text:
                score += 3
            if term in description_text:
                score += 1
            if term in requirements_text:
                score += 1
        return score

    mongo_client = None
    try:
        try:
            limit = int(limit)
        except Exception:  # noqa: BLE001
            limit = 10
        limit = max(1, min(limit, 25))
        requested_limit = limit
        fetch_limit = min(max(limit * 4, limit), 100)

        mongo_client, collections = _connect_jobs_db()
        jobs_collection = collections["jobs"]
        locations_collection = collections["locations"]

        profile = _get_talentcentral_user_profile(collections, user_id)
        user_id_int = profile.get("user_id_int")
        user_city_id = profile.get("user_city_id")
        user_location_text_legacy = profile.get("user_location_text_legacy")
        commute_radius_km = profile.get("commute_radius_km")
        resume_path = profile.get("resume_path")
        print(
            "jobs user context",
            {
                "effective_user_id": str(user_id),
                "user_id_int": user_id_int,
                "user_profile_found": bool(profile.get("user_profile_found")),
                "user_city_id": user_city_id,
                "has_legacy_location_text": bool(user_location_text_legacy),
                "has_resume_path": bool(resume_path),
            },
        )

        if resume_path and not use_profile and _is_vague_job_query(query_text):
            use_profile = True
            print("jobs profile usage overridden", {"reason": "vague_query_with_available_resume", "query_text": query_text, "use_profile": use_profile})

        query_terms = []
        profile_terms = []
        if query_text and str(query_text).strip():
            qt = str(query_text).strip()
            if len(qt) <= 80:
                query_terms.append(qt.lower())
            query_terms.extend(_extract_keywords(qt, max_terms=8))

        if use_profile and resume_path:
            resume_payload = _get_cached_resume_payload(
                user_id=user_id_int if user_id_int is not None else str(user_id),
                resume_path=resume_path,
            )
            resume_text = resume_payload.get("resume_text") or ""
            profile_terms.extend(_extract_keywords(resume_text, max_terms=8))
            print("jobs resume context", {"has_resume_text": bool(resume_text.strip()), "cache_hit": bool(resume_payload.get("cache_hit")), "resume_path_present": bool(resume_path)})

        seen = set()
        deduped_terms = []
        for t in (query_terms + profile_terms):
            key = str(t or "").strip().lower()
            if key and key not in seen:
                seen.add(key)
                deduped_terms.append(key)
        deduped_terms = deduped_terms[:8]

        query_city_ids = _resolve_city_ids_from_query(locations_collection, query_text)
        city_name_tokens = set()
        if query_city_ids:
            city_rows = list(locations_collection.find({"legacyCityId": {"$in": query_city_ids}}, {"city": 1}))
            for row in city_rows:
                city_name = str((row or {}).get("city") or "").strip().lower()
                if not city_name:
                    continue
                city_name_tokens.add(city_name)
                for part in re.findall(r"[a-zA-Z][a-zA-Z0-9\-\+\.]{1,}", city_name):
                    city_name_tokens.add(part.lower())
            if city_name_tokens and deduped_terms:
                generic_job_terms = {
                    "find", "job", "jobs", "role", "roles", "position", "positions",
                    "opportunity", "opportunities", "work", "career", "careers",
                    "near", "nearby", "around", "me",
                }
                filtered_terms = []
                for term in deduped_terms:
                    t = str(term or "").strip().lower()
                    if not t or t in city_name_tokens or t in generic_job_terms:
                        continue
                    phrase_tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9\-\+\.]{1,}", t)
                    meaningful_tokens = [tok for tok in phrase_tokens if tok not in city_name_tokens and tok not in generic_job_terms]
                    if not meaningful_tokens and any(city in t for city in city_name_tokens):
                        continue
                    filtered_terms.append(t)
                if filtered_terms != deduped_terms:
                    print("jobs text terms adjusted for location query", {"original_terms": deduped_terms, "filtered_terms": filtered_terms, "city_tokens": sorted(city_name_tokens)})
                    deduped_terms = filtered_terms

        now = datetime.utcnow()
        where_filter = {
            "status": "active",
            "$or": [{"expiresAt": {"$exists": False}}, {"expiresAt": None}, {"expiresAt": {"$gt": now}}],
        }
        if query_city_ids:
            where_filter["location.cityId"] = {"$in": query_city_ids}
            print("jobs location filter applied", {"query_city_ids": query_city_ids, "query_city_count": len(query_city_ids)})
        else:
            print("jobs location filter not applied", {"reason": "no_city_match_in_query"})

        if deduped_terms:
            text_or = []
            for term in deduped_terms:
                safe_term = re.escape(term)
                text_or.extend([
                    {"title": {"$regex": safe_term, "$options": "i"}},
                    {"description": {"$regex": safe_term, "$options": "i"}},
                    {"requirements": {"$regex": safe_term, "$options": "i"}},
                ])
            where_filter["$and"] = [{"$or": text_or}]

        print("where_filter", where_filter)
        raw_jobs = list(jobs_collection.find(where_filter).limit(fetch_limit))
        results = []
        for job_doc in raw_jobs:
            job = _normalize_job(job_doc)
            if user_city_id is not None and _safe_int(job.get("city_id")) == user_city_id:
                job["location_score"] = 2
            job["relevance_score"] = _calc_relevance(job_doc, deduped_terms)
            results.append(job)

        results.sort(
            key=lambda job: (
                int(job.get("location_score") or 0),
                float(job.get("relevance_score") or 0),
                job.get("created_at").timestamp() if hasattr(job.get("created_at"), "timestamp") else 0,
            ),
            reverse=True,
        )

        if results and (user_city_id is not None or user_location_text_legacy):
            try:
                if commute_radius_km is None or commute_radius_km <= 0:
                    commute_radius_km = 30

                user_geo = None
                user_location_display = None
                if user_city_id is not None:
                    user_city = locations_collection.find_one(
                        {"legacyCityId": user_city_id},
                        {"city": 1, "provinceCode": 1, "latitude": 1, "longitude": 1},
                    ) or {}
                    u_name = user_city.get("city")
                    u_prov = user_city.get("provinceCode")
                    u_lat = user_city.get("latitude")
                    u_lon = user_city.get("longitude")
                    if u_name and u_prov:
                        user_location_display = f"{u_name}, {u_prov}"
                    if u_lat is not None and u_lon is not None:
                        user_geo = (float(u_lat), float(u_lon))

                if not user_geo and user_location_text_legacy:
                    user_geo = _geocode_place(user_location_text_legacy)

                user_loc_lc = str(user_location_display or user_location_text_legacy or "").strip().lower()
                for job in results:
                    job_loc = (job.get("location") or "").strip()
                    job["distance_km"] = None
                    if not job_loc:
                        continue
                    job_city_id = _safe_int(job.get("city_id"))
                    if user_city_id is not None and job_city_id is not None and job_city_id == user_city_id:
                        job["distance_km"] = 0.0
                        job["location_score"] = max(int(job.get("location_score") or 0), 1)
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
                        if not job_geo:
                            job_geo = _geocode_place(job_loc)
                        if job_geo:
                            dist = _haversine_km(user_geo, job_geo)
                            job["distance_km"] = round(dist, 1)
                            if dist <= commute_radius_km:
                                job["location_score"] = max(int(job.get("location_score") or 0), 1)
                                continue
                    if user_loc_lc and user_loc_lc in job_loc.lower():
                        job["location_score"] = max(int(job.get("location_score") or 0), 1)

                results.sort(
                    key=lambda job: (
                        int(job.get("location_score") or 0),
                        float(job.get("relevance_score") or 0),
                        job.get("created_at").timestamp() if hasattr(job.get("created_at"), "timestamp") else 0,
                    ),
                    reverse=True,
                )
            except Exception as e:  # noqa: BLE001
                print(f"Location scoring failed: {e}")

        return results[:requested_limit]
    except Exception as e:  # noqa: BLE001
        print(f"Error searching jobs: {e}")
        return []
    finally:
        if mongo_client:
            mongo_client.close()


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
