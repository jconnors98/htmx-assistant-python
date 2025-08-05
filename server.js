// server.js
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import dotenv from "dotenv";
import { OpenAI } from "openai";
import { marked } from "marked";
import sanitizeHtml from "sanitize-html";
import { askGemini } from "./gemini.js"; // make sure this file exists

dotenv.config();

// Validate keys
if (!process.env.OPENAI_API_KEY || !process.env.GEMINI_API_KEY) {
  console.error("❌ Missing API keys. Check your .env file.");
  process.exit(1);
}

// Setup __dirname workaround
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Init Express
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

// Init OpenAI
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// POST /ask — handle chat messages
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
    // Run GPT and Gemini simultaneously
    const [gptResult, geminiResult] = await Promise.all([
      openai.chat.completions.create({
        model: "gpt-4",
        messages: [
          {
            role: "system",
            content: `You're a helpful, warm assistant supporting users on the TalentCentral platform. Help with construction jobs, training, and workforce programs in BC. Speak naturally.`,
          },
          {
            role: "user",
            content: message,
          },
        ],
      }),
      askGemini(message),
    ]);

    const gptText = gptResult.choices?.[0]?.message?.content || "🤖 GPT had no response.";
    const geminiText = geminiResult || "🤖 Gemini had no response.";

    // Markdown → HTML → sanitize
    const format = (text) =>
      sanitizeHtml(marked.parse(text), {
        allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
        allowedAttributes: {
          a: ["href", "target", "rel"],
          img: ["src", "alt"],
        },
      });

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble">
          <strong>🔮 GPT says:</strong>
          <div class="markdown">${format(gptText)}</div>
          <hr/>
          <strong>🌐 Gemini says:</strong>
          <div class="markdown">${format(geminiText)}</div>
          <div class="source-tag">🔗 Blended from GPT + Gemini</div>
        </div>
      </div>
    `;

    res.send(html);
  } catch (err) {
    console.error("❌ Error fetching AI responses:", err);
    res.send(`
      <div class="chat-entry assistant">
        <div class="bubble">❌ There was an error getting a response from the assistant.</div>
      </div>
    `);
  }
});

// Serve frontend
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Start server
app.listen(port, () => {
  console.log(`✅ Assistant is live at http://localhost:${port}`);
});
