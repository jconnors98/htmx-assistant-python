from __future__ import annotations

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
    ) -> None:
        self.client = client
        self.conversations = db.get_collection("conversations")
        self.messages = db.get_collection("messages")
        self.summaries = db.get_collection("summaries")
        self.modes = modes_collection
        self.vector_store_id = vector_store_id
        self.max_messages = max_messages

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
        summary = summary_doc.get("summary_text", "") if summary_doc else ""

        # Grab the latest 8 messages before the newest user message
        all_msgs = list(
            self.messages.find({"conversation_id": conv_id})
            .sort("created_at", -1)
            .limit(9)
        )
        # first item is the latest user message, the rest are previous turns
        recent_history = list(reversed(all_msgs[1:]))
        recent_history = recent_history[-8:]  # last 4 turns (8 messages)

        recent_text = "\n".join(
            f"{m['role'].capitalize()}: {m['content']}" for m in recent_history
        )

        context = f"<SUMMARY>\n{summary}\n<RECENT_TURNS>\n{recent_text}\n"
        return context

    def _build_prompt_and_tools(
        self, mode: str, tag: str
    ) -> Tuple[str, List[Dict], List[Dict[str, Any]]]:
        mode_doc = self.modes.find_one({"name": mode}) if mode else None
        mode_context = mode_doc.get("description", "") if mode_doc else ""
        mode_preferred_sites = mode_doc.get("preferred_sites", []) if mode_doc else []
        mode_blocked_sites = mode_doc.get("blocked_sites", []) if mode_doc else []
        allow_other_sites = mode_doc.get("allow_other_sites", True) if mode_doc else True
        priority_source = mode_doc.get("priority_source", "sites") if mode_doc else "sites"
        tags = mode_doc.get("tags", []) if mode_doc else []
        database_config = mode_doc.get("database") if mode_doc else None

        tools: List[Dict] = []
        data_sources: List[Dict[str, Any]] = []
        interest = mode_doc.get("title", "general BCCA information") if mode_doc else "general BCCA information"
        gpt_system_prompt = (
            "You're a helpful, warm assistant supporting users with information about the British Columbia construction industry. "
            f"The user is interested in {interest}. {mode_context}. "
        )
        if self.vector_store_id and mode:
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

        if self.vector_store_id and mode:
            gpt_system_prompt += "You have access to a vector store containing relevant documents. "
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

        if database_config:
            db_source = self._build_database_data_source(database_config)
            if db_source:
                data_sources.append(db_source)
                gpt_system_prompt += (
                    "You also have access to a structured database configured for this mode."
                    " Use it to look up precise information when needed. "
                )

        if allow_other_sites and mode_preferred_sites:
            gpt_system_prompt += "If you cannot fully answer from these, then use other reputable sources. "
            if mode_blocked_sites:
                gpt_system_prompt += f"Do not use the following sites as a source: {', '.join(mode_blocked_sites)} "

            gpt_system_prompt += "In your final answer, list internet sources from my preferred sites before listing any other internet sources."
            
        elif mode_preferred_sites:
            gpt_system_prompt += (
                "Only use these websites in your web search; do not use any other sources from the internet."
            )
        
        gpt_system_prompt += "Do not repeat information."

        tools.append({"type": "web_search"})

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
    ) -> Tuple[str, str, Optional[dict]]:
        conv_id = ObjectId(conversation_id)
        self.add_user_message(conversation_id, user_id, text)
        context = self._build_context(conversation_id)
        system_prompt, tools, data_sources = self._build_prompt_and_tools(mode, tag)
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

        response = _call_model("gpt-4.1-mini")
        if not _is_confident(response):
            print("response from mini model is not confident, calling gpt-4.1")
            response = _call_model("gpt-4.1")
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
    
    # Database helpers ---------------------------------------------------
    def _build_database_data_source(
        self, database_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(database_config, dict):
            return None

        if "type" in database_config and "config" in database_config:
            config = database_config.get("config") or {}
            connection_uri = config.get("connection_uri")
            if connection_uri:
                return {
                    "type": database_config["type"],
                    "config": config,
                }
            # Fall through to attempt to construct a URI if missing.

        db_type = (database_config.get("type") or "").lower()
        if db_type in {"postgres", "postgresql"}:
            connection_uri = database_config.get("connection_uri")
            if not connection_uri:
                connection_uri = self._build_postgres_uri(database_config)
            if not connection_uri:
                return None

            config: Dict[str, Any] = {}
            config.update(database_config.get("config") or {})
            config["connection_uri"] = connection_uri
            schemas = database_config.get("schemas")
            if schemas:
                config.setdefault("schemas", schemas)
            search_path = database_config.get("search_path")
            if search_path:
                config.setdefault("search_path", search_path)
            max_rows = database_config.get("max_rows")
            if max_rows:
                config.setdefault("max_rows", max_rows)

            return {"type": "postgresql", "config": config}

        return None

    def _build_postgres_uri(self, database_config: Dict[str, Any]) -> Optional[str]:
        required = ["host", "database", "username", "password"]
        if not all(database_config.get(key) for key in required):
            return None

        host = database_config["host"]
        port = database_config.get("port")
        username = quote_plus(str(database_config["username"]))
        password = quote_plus(str(database_config["password"]))
        db_name = database_config["database"]

        query_params: Dict[str, Any] = {}
        sslmode = database_config.get("sslmode")
        if sslmode:
            query_params["sslmode"] = sslmode
        options = database_config.get("options")
        extra_query = ""
        if isinstance(options, dict):
            for key, value in options.items():
                if value is not None:
                    query_params[key] = value
        if query_params:
            extra_query = urlencode(query_params, doseq=True)
        elif isinstance(options, str) and options:
            extra_query = options.lstrip("?")

        query_string = f"?{extra_query}" if extra_query else ""
        port_part = f":{port}" if port else ""
        return f"postgresql://{username}:{password}@{host}{port_part}/{db_name}{query_string}"

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