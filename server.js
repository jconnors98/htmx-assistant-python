// server.js
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import dotenv from "dotenv";
import { OpenAI } from "openai";
import { marked } from "marked";
import sanitizeHtml from "sanitize-html";
import multer from "multer";
import fs from "fs";
import { askGemini } from "./gemini.js";
import { parseResume } from "./parsePDF.js"; // PDF parser

dotenv.config();

// Validate API keys
if (!process.env.OPENAI_API_KEY || !process.env.GEMINI_API_KEY) {
  console.error("❌ Missing API keys. Check your .env file.");
  process.exit(1);
}

// Setup Express
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

// Secure headers
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  next();
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Markdown formatting helper
const format = (text) =>
  sanitizeHtml(marked.parse(text), {
    allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
    allowedAttributes: { a: ["href", "target", "rel"], img: ["src", "alt"] },
  });

// 🧠 Chat route with routing logic
app.post("/ask", async (req, res) => {
  const message = req.body.message?.trim();
  if (!message) {
    return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ Message is required.</div></div>`);
  }

  try {
    let gptText = "";
    let geminiText = "";

    const taskKeywords = /\b(resume|cover letter|cv|application|write|rewrite|reword|organize|format|polish|edit|revise|improve|draft|summarize)\b/i;

    if (taskKeywords.test(message)) {
      const gptResult = await openai.chat.completions.create({
        model: "gpt-4",
        messages: [
          {
            role: "system",
            content: `You're a helpful assistant supporting construction job seekers in BC. Help write resumes, cover letters, prep interviews, rewrite drafts, organize resume content, and give feedback.`,
          },
          { role: "user", content: message },
        ],
      });
      gptText = gptResult.choices?.[0]?.message?.content || "🤖 GPT had no response.";
    } else {
      geminiText = await askGemini(message);
    }

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble">
          ${gptText ? `<strong>🔧 GPT (Task Helper):</strong><div class="markdown">${format(gptText)}</div>` : ""}
          ${geminiText ? `<strong>🌐 Gemini (Search Bot):</strong><div class="markdown">${format(geminiText)}</div>` : ""}
        </div>
      </div>
    `;

    res.send(html);
  } catch (err) {
    console.error("❌ Error fetching AI responses:", err);
    res.send(`<div class="chat-entry assistant"><div class="bubble">❌ There was an error getting a response.</div></div>`);
  }
});

// 📝 Resume Upload Route
const upload = multer({ dest: "uploads/" });

app.post("/upload", upload.single("resume"), async (req, res) => {
  if (!req.file) {
    return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ Please upload a PDF file.</div></div>`);
  }

  try {
    const resumeText = await parseResume(req.file.path);
    fs.unlinkSync(req.file.path); // Clean up uploaded file

    const gptResult = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        {
          role: "system",
          content: `You're a resume coach for construction jobs in BC. Give clear, supportive feedback and suggestions.`,
        },
        {
          role: "user",
          content: `Please review the following resume:\n\n${resumeText}`,
        },
      ],
    });

    const gptText = gptResult.choices?.[0]?.message?.content || "🤖 GPT had no response.";

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble">
          <strong>📄 Resume Review:</strong>
          <div class="markdown">${format(gptText)}</div>
        </div>
      </div>
    `;

    res.send(html);
  } catch (err) {
    console.error("❌ Resume review failed:", err);
    res.send(`<div class="chat-entry assistant"><div class="bubble">❌ Error reviewing resume.</div></div>`);
  }
});

// Fallback to frontend
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Start server
app.listen(port, () => {
  console.log(`✅ Assistant is live at http://localhost:${port}`);
});
