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
import { parseResume } from "./parsePDF.js"; // PDF parser

dotenv.config();

if (!process.env.OPENAI_API_KEY) {
  console.error("❌ Missing OpenAI API key.");
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

// Markdown sanitizer
const format = (text) =>
  sanitizeHtml(marked.parse(text), {
    allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
    allowedAttributes: { a: ["href", "target", "rel"], img: ["src", "alt"] },
  });


// 🧠 Chat route (Unified GPT with responses API)
app.post("/ask", async (req, res) => {
  const message = req.body.message?.trim();
  if (!message) {
    return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ Message is required.</div></div>`);
  }

  try {
    const taskKeywords = /\b(resume|cover letter|cv|application|write|rewrite|reword|organize|format|polish|edit|revise|improve|draft|summarize)\b/i;

    const useSearch = !taskKeywords.test(message);

    const response = await openai.responses.create({
      model: "gpt-4o",
      messages: [
        {
          role: "system",
          content: useSearch
            ? "You are a smart assistant who can look things up online and summarize or explain them clearly."
            : "You're a helpful assistant for construction job seekers in BC. Help with writing resumes, cover letters, organizing drafts, summarizing experience, and improving formatting.",
        },
        { role: "user", content: message }
      ],
      tools: useSearch ? [{ type: "web_search" }] : []
    });

    const reply = response.choices?.[0]?.message?.content || "🤖 No response available.";

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble">
          <div class="markdown">${format(reply)}</div>
        </div>
      </div>
    `;
    res.send(html);

  } catch (err) {
    console.error("❌ /ask error:", err);
    res.send(`<div class="chat-entry assistant"><div class="bubble">❌ There was an error getting a response.</div></div>`);
  }
});


// 📄 Resume Upload
const upload = multer({ dest: "uploads/" });

app.post("/upload", upload.single("resume"), async (req, res) => {
  if (!req.file) {
    return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ Please upload a PDF resume file.</div></div>`);
  }

  const allowedTypes = ["application/pdf"];
  if (!allowedTypes.includes(req.file.mimetype)) {
    fs.unlinkSync(req.file.path);
    return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ Only PDF files are supported.</div></div>`);
  }

  try {
    const resumeText = await parseResume(req.file.path);
    fs.unlinkSync(req.file.path); // Clean up temp file

    const gptResult = await openai.responses.create({
      model: "gpt-4o",
      messages: [
        {
          role: "system",
          content: "You're a resume coach for construction jobs in BC. Review the resume, improve clarity and formatting, and provide feedback in Markdown.",
        },
        {
          role: "user",
          content: `Please review and improve this resume:\n\n${resumeText}`
        }
      ]
    });

    const gptText = gptResult.choices?.[0]?.message?.content || "🤖 No response from assistant.";

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
    console.error("❌ Resume upload error:", err);
    res.send(`<div class="chat-entry assistant"><div class="bubble">❌ Error reviewing the resume.</div></div>`);
  }
});


// Frontend Fallback
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Start server
app.listen(port, () => {
  console.log(`✅ Assistant is live at http://localhost:${port}`);
});
