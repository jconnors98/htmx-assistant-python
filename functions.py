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
                    top_prompts = _get_unique_prompts_data(pipeline, match, limit, prompt_logs_collection)
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
                return {"response": final_response.output_text}
        
        return {"response": response.output_text}
        
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
        
        cursor = cnx.cursor(dictionary=True)
        
        # Search for permits using LIKE with wildcards for partial matches
        search_query = """
            SELECT id, project_description, status, source, 
                   date_added, permit_number
            FROM permit_data 
            WHERE project_description LIKE %s 
            ORDER BY updated_at DESC 
            LIMIT %s
        """
        
        # Add wildcards for partial matching
        search_pattern = f"%{query_text}%"
        cursor.execute(search_query, (search_pattern, limit))
        
        results = cursor.fetchall()
        
        cursor.close()
        cnx.close()
        
        return results
        
    except Exception as e:
        print(f"Error searching permits: {e}")
        return []


def _get_analytics_data_for_query(pipeline, match, prompt_logs_collection, modes_collection):
    """Get relevant analytics data for AI processing."""
    
    # Get basic counts
    total_prompts = prompt_logs_collection.count_documents({**match, "prompt": {"$exists": True}})
    total_responses = prompt_logs_collection.count_documents({**match, "response": {"$exists": True}})
    unique_conversations = len([cid for cid in prompt_logs_collection.distinct("conversation_id", match) if cid])
    unique_users = len([ip for ip in prompt_logs_collection.distinct("ip_hash", match) if ip])
    
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
