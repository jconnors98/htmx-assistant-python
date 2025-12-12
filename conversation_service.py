from __future__ import annotations

import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import quote_plus, urlencode
from bson import ObjectId


class ConversationService:
    """Service for managing conversations and model calls."""

    def __init__(
        self,
        db,
        modes_collection,
        client,
        vector_store_id: Optional[str] = None,
        max_messages: int = 20,
        doc_intelligence_service=None,
        document_intelligence_enabled: bool = False,
    ) -> None:
        self.client = client
        self.conversations = db.get_collection("conversations")
        self.messages = db.get_collection("messages")
        self.summaries = db.get_collection("summaries")
        self.modes = modes_collection
        self.vector_store_id = vector_store_id
        self.max_messages = max_messages
        self.doc_intelligence_service = doc_intelligence_service
        self.document_intelligence_enabled = document_intelligence_enabled
        # Lightweight runtime tuning knobs (speed and cost control)
        self.max_summary_chars = 1200
        self.max_message_chars = 800
        self.max_context_chars = 5000
        self.recent_history_limit = 9  # keep ~4 user/assistant turns, capped by char budget
        self.mode_cache_ttl = 120  # seconds
        self._mode_cache: Dict[str, Dict[str, Any]] = {}

    # Public API ---------------------------------------------------------
    def add_user_message(self, conversation_id: str, user_id: str, text: str) -> None:
        conv_id = ObjectId(conversation_id)
        now = datetime.utcnow()
        # ensure conversation exists
        self.conversations.update_one(
            {"_id": conv_id},
            {
                "$setOnInsert": {"user_id": user_id, "created_at": now},
                "$set": {"updated_at": now},
            },
            upsert=True,
        )
        self.messages.insert_one(
            {
                "conversation_id": conv_id,
                "role": "user",
                "content": text,
                "created_at": now,
            }
        )
        self._enforce_cap(conv_id)

    def store_conversation_files(self, conversation_id: str, file_ids: List[str]) -> None:
        """Store uploaded file IDs with the conversation for future use."""
        conv_id = ObjectId(conversation_id)
        self.conversations.update_one(
            {"_id": conv_id},
            {"$addToSet": {"uploaded_files": {"$each": file_ids}}},
            upsert=True
        )

    def get_conversation_files(self, conversation_id: str) -> List[str]:
        """Get all uploaded file IDs for a conversation."""
        conv_id = ObjectId(conversation_id)
        conv_doc = self.conversations.find_one({"_id": conv_id})
        return conv_doc.get("uploaded_files", []) if conv_doc else []

    def clear_conversation_files(self, conversation_id: str) -> List[str]:
        """Clear and return all uploaded file IDs for a conversation."""
        conv_id = ObjectId(conversation_id)
        conv_doc = self.conversations.find_one({"_id": conv_id})
        file_ids = conv_doc.get("uploaded_files", []) if conv_doc else []
        
        # Remove files from conversation
        self.conversations.update_one(
            {"_id": conv_id},
            {"$unset": {"uploaded_files": ""}}
        )
        
        return file_ids

    def cleanup_old_files(self, days_old: int = 7) -> List[str]:
        """Clean up files from conversations older than specified days."""
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        old_conversations = self.conversations.find({
            "updated_at": {"$lt": cutoff_date},
            "uploaded_files": {"$exists": True, "$ne": []}
        })
        
        all_old_files = []
        for conv in old_conversations:
            file_ids = conv.get("uploaded_files", [])
            all_old_files.extend(file_ids)
            
            # Remove files from conversation
            self.conversations.update_one(
                {"_id": conv["_id"]},
                {"$unset": {"uploaded_files": ""}}
            )
        
        return all_old_files

    def _build_context(self, conversation_id: str) -> str:
        """Build summary and recent history context for the model."""
        conv_id = ObjectId(conversation_id)
        summary_doc = self.summaries.find_one({"conversation_id": conv_id})
        summary_raw = summary_doc.get("summary_text", "") if summary_doc else ""
        summary = self._truncate_text(summary_raw, self.max_summary_chars)

        # Grab the latest 8 messages before the newest user message
        all_msgs = list(
            self.messages.find({"conversation_id": conv_id})
            .sort("created_at", -1)
            .limit(self.recent_history_limit)
        )
        # first item is the latest user message, the rest are previous turns
        recent_history = list(reversed(all_msgs[1:]))
        recent_text_parts = []
        budget = max(self.max_context_chars - len(summary), 0)
        for message in recent_history:
            # Cap each message and respect an overall budget to keep latency low
            content = self._truncate_text(
                message.get("content", ""), self.max_message_chars
            )
            entry = f"{message.get('role', 'unknown').capitalize()}: {content}"
            if budget - len(entry) < 0:
                break
            recent_text_parts.append(entry)
            budget -= len(entry)

        recent_text = "\n".join(recent_text_parts)

        context = f"<SUMMARY>\n{summary}\n<RECENT_TURNS>\n{recent_text}\n"
        return context

    def _get_mode_doc(self, mode: str) -> Optional[Dict[str, Any]]:
        """Fetch mode config with a small per-process cache to cut DB latency."""
        if not mode:
            return None
        cached = self._mode_cache.get(mode)
        now = time.time()
        if cached and (now - cached["ts"]) < self.mode_cache_ttl:
            return cached["doc"]
        doc = self.modes.find_one({"name": mode})
        self._mode_cache[mode] = {"doc": doc, "ts": now}
        return doc

    @staticmethod
    def _truncate_text(text: str, max_chars: int) -> str:
        """Keep head/tail of long text to preserve intent while trimming tokens."""
        if not text or len(text) <= max_chars:
            return text
        head = text[: max_chars // 2]
        tail = text[-max_chars // 2 :]
        return f"{head} ... {tail}"

    def _build_prompt_and_tools(
        self, mode_doc: Optional[Dict[str, Any]], mode: str, tag: str
    ) -> Tuple[str, List[Dict], List[Dict[str, Any]]]:
        mode_context = mode_doc.get("description", "") if mode_doc else ""
        mode_preferred_sites = mode_doc.get("preferred_sites", []) if mode_doc else []
        mode_blocked_sites = mode_doc.get("blocked_sites", []) if mode_doc else []
        allow_other_sites = mode_doc.get("allow_other_sites", True) if mode_doc else True
        priority_source = mode_doc.get("priority_source", "sites") if mode_doc else "sites"
        has_files = mode_doc.get("has_files", False) if mode_doc else False
        has_scraped_content = mode_doc.get("has_scraped_content", False) if mode_doc else False
        tags = mode_doc.get("tags", []) if mode_doc else []
        province = mode_doc.get("province", "British Columbia") if mode_doc else "British Columbia"

        tools: List[Dict] = []
        data_sources: List[Dict[str, Any]] = []
        interest = mode_doc.get("title", "general BCCA information") if mode_doc else "general BCCA information"
        gpt_system_prompt = (
            f"You're a helpful, warm assistant supporting users with information about the {province} construction industry. "
            f"The user is interested in {interest}. {mode_context}. "
        )
        if self.vector_store_id and mode and (has_files or has_scraped_content) and mode != "permitsca":
            file_search_tool: Dict = {
                "type": "file_search",
                "vector_store_ids": [self.vector_store_id],
                "filters": {},
            }
            if tag and tag in tags:
                file_search_tool["filters"] = {
                    "type": "or",
                    "filters": [
                        {
                            "type": "and",
                            "filters": [
                                {"type": "eq", "key": "mode", "value": mode},
                                {"type": "eq", "key": "tag", "value": tag},
                            ],
                        },
                        {
                            "type": "and",
                            "filters": [
                                {"type": "eq", "key": "mode", "value": mode},
                                {"type": "eq", "key": "always_include", "value": "true"},
                            ],
                        },
                    ],
                }
            else:
                file_search_tool["filters"] = {
                    "type": "eq",
                    "key": "mode",
                    "value": mode,
                }
            tools.append(file_search_tool)

            # Build priority instructions based on what content is available
            if has_scraped_content and has_files:
                gpt_system_prompt += "You have access to a vector store containing both scraped website content and uploaded documents. "
            elif has_scraped_content:
                gpt_system_prompt += "You have access to a vector store containing scraped website content from configured sites. "
            elif has_files:
                gpt_system_prompt += "You have access to a vector store containing relevant uploaded documents. "
            
            # Priority logic for scraped content
            if has_scraped_content:
                if priority_source == "files":
                    gpt_system_prompt += (
                        "Always begin by consulting the scraped content and uploaded files in the vector store. "
                        "If you cannot find a complete answer there, then perform live web searches. "
                    )
                elif priority_source == "sites":
                    gpt_system_prompt += (
                        "Always begin by consulting the scraped content in the vector store (which contains up-to-date information from configured sites). "
                        "Only perform live web searches if the scraped content doesn't fully answer the question. "
                    )
                else:
                    gpt_system_prompt += (
                        "Consult the scraped content and vector store first, then use live web searches if needed. "
                    )
            elif has_files:
                # Original file-only logic
                if priority_source == "files":
                    if mode_preferred_sites:
                        gpt_system_prompt += (
                            "Always begin by reviewing the files in the vector store. If you cannot find a complete answer there, then search the preferred websites in order of priority. "
                        )
                    else:
                        gpt_system_prompt += (
                            "Always begin by reviewing the files in the vector store. If you still need more information, expand your search to reputable internet sources. "
                        )
                elif priority_source == "sites" and mode_preferred_sites:
                    gpt_system_prompt += (
                        "Always begin by searching the preferred websites listed below, in order. If those sites do not provide the answer, then review the files in the vector store. " 
                    )
                else:
                    gpt_system_prompt += (
                        "When answering, use both the vector store and the internet. "
                    )
            
            if mode_preferred_sites:
                gpt_system_prompt += (
                    "When searching the internet, search the following websites, listed in order of highest priority first, using the asterisk as a match-all character: "
                    f"{', '.join(mode_preferred_sites)} "
                )
            gpt_system_prompt += "Do not specify the source / file name of uploaded files, or mention them in your response. "
            tools.append({"type": "web_search"})

        elif mode == "permitsca":
            permit_search_tool = {
                "type": "function",
                "name": "search_permits",
                "description": "Search for permits in the database using project description keywords",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search terms to look for in permit project descriptions"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results to return (default: 10)",
                            "default": 10
                        }
                    },
                    "required": ["query"]
                }
            }
            tools.append(permit_search_tool)
            
            # Update system prompt to mention permit search capability
            gpt_system_prompt += "You have access to a permit database search tool. Use the search_permits function to find relevant permits. "

        else:
            if mode_preferred_sites:
                gpt_system_prompt += (
                    "When answering, use the following websites. They are listed in order of highest priority first, using the asterisk as a match-all character: "
                    f"{', '.join(mode_preferred_sites)} "
                )
            else:
                gpt_system_prompt += (
                    "When answering, use the internet. "
                )

            tools.append({"type": "web_search"})

        # if database_config:
        #     db_source = self._build_database_data_source(database_config)
        #     if db_source:
        #         data_sources.append(db_source)
        #         gpt_system_prompt += (
        #             "You also have access to a structured database configured for this mode."
        #             " Use it to look up precise information when needed. "
        #         )

        if allow_other_sites and mode_preferred_sites and mode != "permitsca":
            gpt_system_prompt += "If you cannot fully answer from these, then use other reputable sources. "
            if mode_blocked_sites:
                gpt_system_prompt += f"Do not use the following sites as a source: {', '.join(mode_blocked_sites)} "

            gpt_system_prompt += "In your final answer, list internet sources from my preferred sites before listing any other internet sources. "
            
        elif mode_preferred_sites and mode != "permitsca":
            gpt_system_prompt += (
                "Only use these websites in your web search; do not use any other sources from the internet. "
            )
        
        gpt_system_prompt += "Do not repeat information. When using lists, use bullets versus numbering."

        return gpt_system_prompt, tools, data_sources

    def respond(
        self,
        conversation_id: str,
        user_id: str,
        text: str,
        mode: str = "",
        tag: str = "",
        previous_response_id: Optional[str] = None,
        file_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        doc_intel_session_id: Optional[str] = None,
    ) -> Tuple[str, str, Optional[dict]]:
        conv_id = ObjectId(conversation_id)
        self.add_user_message(conversation_id, user_id, text)
        mode_doc = self._get_mode_doc(mode) if mode else None
        doc_session_id = doc_intel_session_id or conversation_id

        if (
            self.document_intelligence_enabled
            and self.doc_intelligence_service
            and mode_doc
            and mode_doc.get("doc_intelligence_enabled")
        ):
            doc_context = self.doc_intelligence_service.generate_assistant_context(mode_doc, text, doc_session_id)
            if doc_context:
                return self._respond_with_manual_text(conv_id, doc_context, text)

        context = self._build_context(conversation_id)
        system_prompt, tools, data_sources = self._build_prompt_and_tools(mode_doc, mode, tag)

        full_system_prompt = f"{system_prompt}\n{context}"
        
        # Get all files for this conversation (previous + new)
        conversation_files = self.get_conversation_files(conversation_id)
        all_file_ids = list(set((file_ids or []) + conversation_files))  # Combine and deduplicate

        def _call_model(model_name: str):
            user_content: List[Dict] = [
                {"type": "input_text", "text": text}
            ]
            
            # Handle single file_id (backward compatibility)
            if file_id:
                user_content.append({"type": "input_file", "file_id": file_id})
            
            # Handle all file_ids (new + conversation files)
            if all_file_ids:
                for fid in all_file_ids:
                    user_content.append({"type": "input_file", "file_id": fid})

            params = {
                "model": model_name,
                "tools": tools,
                "input": [
                    {"role": "system", "content": full_system_prompt},
                    {"role": "user", "content": user_content},
                ],
            }
            if (
                previous_response_id
                and self.messages.count_documents({"conversation_id": conv_id}) <= 6
            ):
                params["previous_response_id"] = previous_response_id

            if data_sources:
                return self.client.responses.create(
                    **params, extra_body={"data_sources": data_sources}
                )
            
            return self.client.responses.create(**params)

        def _is_confident(res) -> bool:
            try:
                # Check if the response has the expected structure
                if hasattr(res, 'output') and res.output and len(res.output) > 0:
                    output = res.output[0]
                    if hasattr(output, 'content') and output.content and len(output.content) > 0:
                        content = output.content[0]
                        if isinstance(content, dict):
                            confidence = content.get("confidence")
                        else:
                            confidence = getattr(content, "confidence", None)
                        
                        # Return True if confidence is None (no confidence score) or >= 0.5
                        return confidence is None or confidence >= 0.5
                
                # If we can't find confidence info, assume confident
                return True
            except Exception:
                # If anything goes wrong, assume confident to avoid double calls
                return True

        response = _call_model("gpt-5-mini")
        
        called_function = False
        # Handle function calls for permit search (permitsca mode only)
        if mode == "permitsca" and hasattr(response, 'output') and response.output and len(response.output) > 0:
            output = response.output[0]
            if output.type == "function_call":
                function_name = output.name
                function_args = output.arguments
                
                # Parse function arguments
                if isinstance(function_args, str):
                    import json
                    function_args = json.loads(function_args)
                
                # Execute the permit search tool
                if function_name == "search_permits":
                    # Import the tool function from functions.py
                    from functions import _search_permits_tool
                    
                    query_text = function_args.get("query", "")
                    limit = function_args.get("limit", 10)
                    
                    search_results = _search_permits_tool(query_text, limit)
                    
                    # Format results for the AI
                    if search_results:
                        tool_result = f"Found {len(search_results)} permits matching '{query_text}':\n"
                        for i, permit in enumerate(search_results, 1):
                            tool_result += f"{i}. Permit #{permit.get('permit_number', 'N/A')} - {permit.get('project_description', 'No description')[:500]}... (Status: {permit.get('status', 'Unknown')})\n"
                    else:
                        tool_result = f"No permits found matching '{query_text}'"
                    
                    # Get the AI's final response with tool results
                    final_response = self.client.responses.create(
                        model="gpt-5-mini",
                        input=[
                            {"role": "system", "content": full_system_prompt},
                            {"role": "user", "content": text},
                            output,
                            {"output": tool_result, "call_id": output.call_id, "type": "function_call_output"},
                        ],
                    )
                    response = final_response
                    called_function = True
        
        if not _is_confident(response) and not called_function:
            print("response from mini model is not confident, calling gpt-5.1")
            response = _call_model("gpt-5.1")
        output_text = response.output_text
        now = datetime.utcnow()
        usage = response.usage.total_tokens if response.usage else None
        self.messages.insert_one(
            {
                "conversation_id": conv_id,
                "role": "assistant",
                "content": output_text,
                "model": response.model,
                "usage": usage,
                "created_at": now,
            }
        )
        self.conversations.update_one({"_id": conv_id}, {"$set": {"updated_at": now}})
        self._update_summary(conv_id, text, output_text)
        self._enforce_cap(conv_id)
        return output_text, response.id, usage
    

    # Internal helpers ---------------------------------------------------
    def _update_summary(self, conv_id: ObjectId, user_text: str, assistant_text: str) -> None:
        summary_doc = self.summaries.find_one({"conversation_id": conv_id})
        prev_summary = summary_doc.get("summary_text", "") if summary_doc else ""
        prompt = (
            "Update the running conversation summary in 2-6 sentences.\n"
            f"<SUMMARY>\n{prev_summary}\n"
            f"<NEW_TURN>\nUser: {user_text}\nAssistant: {assistant_text}"
        )
        res = self.client.responses.create(model="gpt-4o-mini", input=prompt)
        new_summary = res.output_text.strip()
        self.summaries.update_one(
            {"conversation_id": conv_id},
            {
                "$set": {
                    "summary_text": new_summary,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )

    def _enforce_cap(self, conv_id: ObjectId) -> None:
        count = self.messages.count_documents({"conversation_id": conv_id})
        if count > self.max_messages:
            excess = count - self.max_messages
            old_ids = [
                m["_id"]
                for m in self.messages.find({"conversation_id": conv_id})
                .sort("created_at", 1)
                .limit(excess)
            ]
            if old_ids:
                self.messages.delete_many({"_id": {"$in": old_ids}})

    def _respond_with_manual_text(self, conv_id: ObjectId, assistant_text: str, user_text: str) -> Tuple[str, str, Optional[dict]]:
        response_id = str(ObjectId())
        now = datetime.utcnow()
        usage = None
        self.messages.insert_one(
            {
                "conversation_id": conv_id,
                "role": "assistant",
                "content": assistant_text,
                "model": "doc_intel",
                "usage": usage,
                "created_at": now,
            }
        )
        self.conversations.update_one({"_id": conv_id}, {"$set": {"updated_at": now}})
        self._update_summary(conv_id, user_text, assistant_text)
        self._enforce_cap(conv_id)
        return assistant_text, response_id, usage