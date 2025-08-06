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
import pdfParse from "pdf-parse";
import mammoth from "mammoth";
import { askGemini } from "./gemini.js";

dotenv.config();

if (!process.env.OPENAI_API_KEY || !process.env.GEMINI_API_KEY) {
  console.error("❌ Missing API keys. Check your .env file.");
  process.exit(1);
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  next();
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const upload = multer({ dest: "uploads/" });

// Ask route (chat input)
app.post("/ask", async (req, res) => {
  const message = req.body.message?.trim();
  if (!message) {
    return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ Message is required.</div></div>`);
  }

  try {
    let gptText = "";
    let geminiText = "";

    // Route based on task or search intent
    if (message.match(/resume|cover letter|interview|application|write|cv|draft/i)) {
      const gptResult = await openai.chat.completions.create({
        model: "gpt-4",
        messages: [
          {
            role: "system",
            content: `You're a helpful assistant supporting construction job seekers. Help generate resumes, cover letters, interview prep, etc. in clear and supportive language.`,
          },
          { role: "user", content: message },
        ],
      });
      gptText = gptResult.choices?.[0]?.message?.content || "🤖 GPT had no response.";
    } else {
      geminiText = await askGemini(message);
    }

    const format = (text) =>
      sanitizeHtml(marked.parse(text), {
        allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
        allowedAttributes: { a: ["href", "target", "rel"], img: ["src", "alt"] },
      });

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
    res.send(`<div class="chat-entry assistant"><div class="bubble">❌ There was an error getting a response from the assistant.</div></div>`);
  }
});

// Upload resume file route
app.post("/upload", upload.single("resume"), async (req, res) => {
  const file = req.file;
  if (!file) {
    return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ No file uploaded.</div></div>`);
  }

  try {
    let resumeText = "";

    if (file.mimetype === "application/pdf") {
      const data = await fs.promises.readFile(file.path);
      const parsed = await pdfParse(data);
      resumeText = parsed.text;
    } else if (
      file.mimetype === "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ) {
      const data = await fs.promises.readFile(file.path);
      const parsed = await mammoth.extractRawText({ buffer: data });
      resumeText = parsed.value;
    } else if (file.mimetype === "text/plain") {
      resumeText = await fs.promises.readFile(file.path, "utf-8");
    } else {
      return res.send(`<div class="chat-entry assistant"><div class="bubble">⚠️ Unsupported file type.</div></div>`);
    }

    fs.unlink(file.path, () => {}); // cleanup

    const gptResult = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        {
          role: "system",
          content: `You're a resume reviewer for construction workers and apprentices. Give constructive feedback. Highlight improvements, missing sections, and clarity.`,
        },
        {
          role: "user",
          content: `Here is my resume:\n\n${resumeText}`,
        },
      ],
    });

    const gptText = gptResult.choices?.[0]?.message?.content || "🤖 GPT had no response.";
    const format = (text) =>
      sanitizeHtml(marked.parse(text), {
        allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
        allowedAttributes: { a: ["href", "target", "rel"], img: ["src", "alt"] },
      });

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble">
          <strong>📄 Resume Feedback (via Upload):</strong>
          <div class="markdown">${format(gptText)}</div>
        </div>
      </div>
    `;

    res.send(html);
  } catch (err) {
    console.error("❌ Resume review error:", err);
    res.send(`<div class="chat-entry assistant"><div class="bubble">❌ Error reading file.</div></div>`);
  }
});

// Serve frontend
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(port, () => {
  console.log(`✅ Assistant is live at http://localhost:${port}`);
});
