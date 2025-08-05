// server.js
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import dotenv from "dotenv";
import { OpenAI } from "openai";
import { marked } from "marked";
import sanitizeHtml from "sanitize-html";

dotenv.config();

// Setup __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Validate API key
if (!process.env.OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY is missing");
  process.exit(1);
}

// Init OpenAI client
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Init Express
const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

// Security headers
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  next();
});

// 🔍 POST /ask – powered by Responses API + Web Search
app.post("/ask", async (req, res) => {
  const message = req.body.message?.trim();
  if (!message) {
    return res.send(`
      <div class="chat-entry assistant">
        <div class="bubble"⚠️ Message is required.</div>
      </div>
    `);
  }

  try {
    const response = await openai.responses.create({
      model: "gpt-4o", // or "gpt-4-turbo"
      instructions: `You are a friendly and resourceful assistant helping users with jobs, training, and apprenticeships in British Columbia. Use web search when needed, and respond conversationally.`,
      tools: [{ type: "tool", tool_name: "web_search" }],
      messages: [{ role: "user", content: message }]
    });

    const result = response.result?.content?.[0]?.text || "🤖 No response.";
    const htmlReply = sanitizeHtml(marked.parse(result), {
      allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
      allowedAttributes: {
        a: ["href", "target", "rel"],
        img: ["src", "alt"]
      }
    });

    res.send(`
      <div class="chat-entry assistant">
        <div class="bubble markdown">${htmlReply}</div>
        <div class="source-tag">🌐 Powered by GPT + Web Search</div>
      </div>
    `);
  } catch (err) {
    console.error("❌ Web Search API Error:", err);
    res.send(`
      <div class="chat-entry assistant">
        <div class="bubble">❌ Error getting assistant response. Try again later.</div>
      </div>
    `);
  }
});

// Serve UI
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Start server
app.listen(port, () => {
  console.log(`✅ Assistant live at http://localhost:${port}`);
});
