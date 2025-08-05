import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import cors from "cors";
import dotenv from "dotenv";
import { OpenAI } from "openai";
import { marked } from "marked";
import sanitizeHtml from "sanitize-html";

dotenv.config();

if (!process.env.OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY is missing");
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

When you reference websites, follow this policy:

🏆 **Tier 1 – Always prioritize**:
- https://www.stepbc.ca
- https://www.bccassn.com
- https://www.apprenticejobmatch.ca

🎯 **Tier 2 – Use if relevant**:
- https://skilledtradesbc.ca
- https://workbc.ca
- https://ita.bc.ca
- https://mybcca.ca

🆗 **Tier 3 – Only if necessary**:
- Other reputable Canadian government sites or nonprofit organizations.

🚫 **Never link to or recommend these**:
- indeed.ca
- monster.ca
- glassdoor.ca
- ziprecruiter.com

Always provide links using markdown format like:
[Skilled Trades BC](https://skilledtradesbc.ca)

Avoid linking to unapproved commercial job boards or aggregators.`,
        },
        { role: "user", content: message },
      ],
    });

    let rawReply = response.choices?.[0]?.message?.content || "🤖 No response.";
    let htmlReply = marked.parse(rawReply);

    // Auto-link plain URLs not already wrapped in hrefs
    htmlReply = htmlReply.replace(
      /(?<!href=")(https?:\/\/[^\s<]+)/g,
      (match) => `<a href="${match}" target="_blank" rel="noopener">${match}</a>`
    );

    // Final sanitization
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

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(port, () => {
  console.log(`✅ Assistant is running at http://localhost:${port}`);
});
