// server.js — Node + HTMX + OpenAI integration

import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import dotenv from "dotenv";
import { OpenAI } from "openai";
import { marked } from "marked";
import sanitizeHtml from "sanitize-html";

// Load environment variables
dotenv.config();

// Check for OpenAI API Key
if (!process.env.OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY is missing in .env");
  process.exit(1);
}

// Setup __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Initialize Express app
const app = express();
const port = process.env.PORT || 3000;

// Global Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

// Add basic security headers
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  next();
});

// Initialize OpenAI
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// HTMX endpoint for chat interaction
app.post("/ask", async (req, res) => {
  const message = req.body.message?.trim();

  if (!message) {
    return res.send(`
      <div class="chat-entry assistant">
        <div class="bubble">⚠️ Message is required.</div>
      </div>
    `);
  }

  try {
    const response = await openai.chat.completions.create({
      model: "gpt-4", // or "gpt-3.5-turbo"
      messages: [
        {
          role: "system",
          content: `You are a helpful assistant for the TalentCentral platform.
You help users find construction jobs, training programs, and resources in British Columbia.`,
        },
        {
          role: "user",
          content: message,
        },
      ],
    });

    const rawReply = response.choices?.[0]?.message?.content || "🤖 No response.";
    const htmlReply = sanitizeHtml(marked.parse(rawReply)); // Markdown → safe HTML

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble">
          <strong>You:</strong> ${message}<br/>
          <strong>Assistant:</strong>
          <div class="markdown">${htmlReply}</div>
          <div class="source-tag">Powered by OpenAI</div>
        </div>
      </div>
    `;

    res.send(html);
  } catch (err) {
    console.error("❌ Error calling OpenAI:", err);
    res.send(`
      <div class="chat-entry assistant">
        <div class="bubble">❌ Error getting response from assistant.</div>
      </div>
    `);
  }
});

// Fallback route to serve index.html for any unknown GET
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Start server
app.listen(port, () => {
  console.log(`✅ Assistant is running at http://localhost:${port}`);
});
