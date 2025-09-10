from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple, List, Dict
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

    def _build_prompt_and_tools(self, mode: str, tag: str) -> Tuple[str, List[Dict]]:
        mode_doc = self.modes.find_one({"name": mode}) if mode else None
        mode_context = mode_doc.get("description", "") if mode_doc else ""
        mode_preferred_sites = mode_doc.get("preferred_sites", []) if mode_doc else []
        mode_blocked_sites = mode_doc.get("blocked_sites", []) if mode_doc else []
        allow_other_sites = mode_doc.get("allow_other_sites", True) if mode_doc else True
        prioritize_files = mode_doc.get("prioritize_files", False) if mode_doc else False
        tags = mode_doc.get("tags", []) if mode_doc else []

        tools: List[Dict] = []
        interest = mode if mode else "general BCCA information"
        gpt_system_prompt = (
            "You're a helpful, warm assistant supporting users with information about the BC Construction Association. "
            f"The user is interested in {interest}. {mode_context} "
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
            if prioritize_files:
                gpt_system_prompt += (
                    "When answering, prioritize information from the vector store first. "  
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

        if allow_other_sites and mode_preferred_sites:
            gpt_system_prompt += (
                "If you cannot fully answer from these, then use other reputable sources. "
                f"Do not use the following sites as a source: {', '.join(mode_blocked_sites)} "
                "In your final answer, list internet sources from my preferred sites before listing any other internet sources."
            )
        elif mode_preferred_sites:
            gpt_system_prompt += (
                "Only use these websites in your web search; do not use any other sources from the internet."
            )

        tools.append({"type": "web_search_preview"})

        print("System prompt:", gpt_system_prompt)

        return gpt_system_prompt, tools

    def respond(
        self,
        conversation_id: str,
        user_id: str,
        text: str,
        mode: str = "",
        tag: str = "",
        previous_response_id: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> Tuple[str, str, Optional[dict]]:
        conv_id = ObjectId(conversation_id)
        self.add_user_message(conversation_id, user_id, text)
        context = self._build_context(conversation_id)
        system_prompt, tools = self._build_prompt_and_tools(mode, tag)
        full_system_prompt = f"{system_prompt}\n{context}"

        def _call_model(model_name: str):
            user_content: List[Dict] = [
                {"type": "input_text", "text": text}
            ]
            if file_id:
                user_content.append({"type": "input_file", "file_id": file_id})

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
            return self.client.responses.create(**params)

        def _is_confident(res) -> bool:
            try:
                content = res.output[0].content[0]
                confidence = (
                    content.get("confidence")
                    if isinstance(content, dict)
                    else getattr(content, "confidence", None)
                )
                return confidence is None or confidence >= 0.5
            except Exception:
                return True

        response = _call_model("gpt-4.1-mini")
        if not _is_confident(response):
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