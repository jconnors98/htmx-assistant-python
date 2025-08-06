// server.js
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import dotenv from "dotenv";
import { OpenAI } from "openai";
import { marked } from "marked";
import sanitizeHtml from "sanitize-html";
import { askGemini } from "./gemini.js";

dotenv.config();

// Validate keys
if (!process.env.OPENAI_API_KEY || !process.env.GEMINI_API_KEY) {
  console.error("❌ Missing API keys. Check your .env file.");
  process.exit(1);
}

// Setup __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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

// Init OpenAI client
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// POST /ask — handle chat
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
    // Run GPT and Gemini in parallel
    const [gptResult, geminiText] = await Promise.all([
      openai.chat.completions.create({
        model: "gpt-4",
        messages: [
          {
            role: "system",
            content: `You're a helpful, warm assistant supporting users on the TalentCentral platform. Help with construction jobs, training, and workforce programs in BC. Speak naturally.`,
          },
          { role: "user", content: message },
        ],
      }),
      askGemini(message),
    ]);

    const gptText = gptResult.choices?.[0]?.message?.content || "🤖 GPT had no response.";
    const geminiContent = geminiText || "🤖 Gemini had no response.";

    // Ask GPT to blend the two responses
    const blended = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        {
          role: "system",
          content:
            "You're a writing assistant. Combine the two answers into a clear, helpful, friendly response for users asking about construction careers or training in BC. Do not repeat points. Include links in markdown if available.",
        },
        {
          role: "user",
          content: `Blend these two answers:\n\n🔮 GPT says:\n${gptText}\n\n🌐 Gemini says:\n${geminiContent}`,
        },
      ],
    });

    const finalReply = blended.choices?.[0]?.message?.content || "🤖 Could not blend results.";
    let htmlReply = marked.parse(finalReply);

    // Auto-link any unwrapped URLs
    htmlReply = htmlReply.replace(
      /(?<!href=")(https?:\/\/[^\s<]+)/g,
      (url) => `<a href="${url}" target="_blank" rel="noopener">${url}</a>`
    );

    // Sanitize the response
    htmlReply = sanitizeHtml(htmlReply, {
      allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
      allowedAttributes: {
        a: ["href", "target", "rel"],
        img: ["src", "alt"],
      },
    });

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble markdown">
          ${htmlReply}
          <div class="source-tag">Powered by BCCA</div>
        </div>
      </div>
    `;

    res.send(html);
  } catch (err) {
    console.error("❌ Error blending AI responses:", err);
    res.send(`
      <div class="chat-entry assistant">
        <div class="bubble">❌ There was an error getting a response. Please try again.</div>
      </div>
    `);
  }
});

// Serve index.html
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(port, () => {
  console.log(`✅ Assistant is live at http://localhost:${port}`);
});
