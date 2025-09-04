from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

from bson import ObjectId


class ConversationService:
    """Service for managing conversations and model calls."""

    def __init__(self, db, client, max_messages: int = 20) -> None:
        self.client = client
        self.conversations = db.get_collection("conversations")
        self.messages = db.get_collection("messages")
        self.summaries = db.get_collection("summaries")
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

    def build_input(self, conversation_id: str, user_text: str) -> Tuple[str, str]:
        """Build system instructions and input for the model."""
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

        input_text = (
            f"<SUMMARY>\n{summary}\n"
            f"<RECENT_TURNS>\n{recent_text}\n"
            f"<USER>\n{user_text}"
        )
        instructions = (
            "You are a helpful assistant. Use the summary and recent turns to"
            " respond to the user."
        )
        return instructions, input_text

    def respond(
        self,
        conversation_id: str,
        user_id: str,
        text: str,
        previous_response_id: Optional[str] = None,
    ) -> Tuple[str, str, Optional[dict]]:
        conv_id = ObjectId(conversation_id)
        self.add_user_message(conversation_id, user_id, text)
        instructions, input_text = self.build_input(conversation_id, text)
        params = {
            "model": "gpt-4.1-mini",
            "input": [
                {"role": "system", "content": instructions},
                {"role": "user", "content": input_text},
            ],
        }
        # Only use previous_response_id for very short follow-ups
        if previous_response_id and self.messages.count_documents({"conversation_id": conv_id}) <= 6:
            params["previous_response_id"] = previous_response_id

        response = self.client.responses.create(**params)
        output_text = response.output_text
        now = datetime.utcnow()
        self.messages.insert_one(
            {
                "conversation_id": conv_id,
                "role": "assistant",
                "content": output_text,
                "model": response.model,
                "usage": getattr(response, "usage", None),
                "created_at": now,
            }
        )
        self.conversations.update_one({"_id": conv_id}, {"$set": {"updated_at": now}})
        self._update_summary(conv_id, text, output_text)
        self._enforce_cap(conv_id)
        return output_text, response.id, getattr(response, "usage", None)

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