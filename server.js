// server.js — Node + HTMX + OpenAI integration

import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import dotenv from "dotenv";
import { OpenAI } from "openai";
import { marked } from "marked";

// Load environment variables
dotenv.config();

// Initialize Express app
const app = express();
app.use(cors());
app.use(express.static('public')); // ✅ THIS IS CRUCIAL
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const port = process.env.PORT || 3000;

// Workaround for __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Serve static frontend (HTML + CSS)
app.use(express.static(path.join(__dirname, "public")));

// Initialize OpenAI with API key from .env
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// POST /ask — receives message from HTMX and responds with assistant reply
app.post("/ask", async (req, res) => {
  const message = req.body.message?.trim();

  if (!message) {
    return res.send("<div class='message assistant'>⚠️ Message is required.</div>");
  }

  try {
    // Call OpenAI (GPT-4 or 3.5)
    const response = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        {
          role: "system",
          content: `You are a helpful assistant for the TalentCentral platform.
You help users find construction jobs, training programs, and resources in British Columbia.`
        },
        {
          role: "user",
          content: message
        }
      ]
    });

    const rawReply = response.choices?.[0]?.message?.content || "🤖 No response.";
    const assistantReply = marked.parse(rawReply); // Convert markdown to HTML

    const html = `
      <div class="message assistant">
        <strong>You:</strong> ${message}<br/>
        <strong>Assistant:</strong>
        <div class="markdown">${assistantReply}</div>
        <div class="source-tag">Powered by OpenAI</div>
      </div>
    `;

    res.send(html);
  } catch (err) {
    console.error("❌ Error calling OpenAI:", err);
    res.send("<div class='message assistant'>❌ Error getting response from assistant.</div>");
  }
});

// Fallback route for unknown GETs — return HTMX UI
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Start server
app.listen(port, () => {
  console.log(`✅ Assistant is running at http://localhost:${port}`);
});
