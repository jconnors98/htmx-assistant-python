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
  console.error("❌ OPENAI_API_KEY is missing in environment");
  process.exit(1);
}

// Setup __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Initialize Express app
const app = express();
const port = process.env.PORT || 3000;

// Middleware
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

// OpenAI client
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Chat endpoint
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
      model: "gpt-4",
      messages: [
        {
          role: "system",
          content: `You are a helpful assistant for the TalentCentral platform.
You help users find construction jobs, training programs, and resources in British Columbia.

When you mention a program, website, or organization, include a clickable markdown link if possible.

Examples:
- [STEP](https://www.stepbc.ca)
- [BCCA](https://www.bccassn.com)
- [Apprentice Job Match](https://www.apprenticejobmatch.ca)`,
        },
        {
          role: "user",
          content: message,
        },
      ],
    });

    let rawReply = response.choices?.[0]?.message?.content || "🤖 No response.";

    // Parse markdown to HTML
    let htmlReply = marked.parse(rawReply);

    // Convert markdown links to open in new tab
    htmlReply = htmlReply.replace(
      /<a href="([^"]+)">/g,
      `<a href="$1" target="_blank" rel="noopener">`
    );

    // Auto-link plain URLs (e.g., www.site.com or https://site.com)
    htmlReply = htmlReply.replace(
      /((https?:\/\/|www\.)[^\s<]+)/g,
      (match) => {
        const url = match.startsWith("http") ? match : `https://${match}`;
        return `<a href="${url}" target="_blank" rel="noopener">${match}</a>`;
      }
    );

    // Sanitize final HTML
    htmlReply = sanitizeHtml(htmlReply, {
      allowedTags: sanitizeHtml.defaults.allowedTags.concat(["img"]),
      allowedAttributes: {
        a: ["href", "target", "rel"],
        img: ["src", "alt"]
      }
    });

    const html = `
      <div class="chat-entry assistant">
        <div class="bubble">
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

// Fallback for other routes
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Start server
app.listen(port, () => {
  console.log(`✅ Assistant is running at http://localhost:${port}`);
});
