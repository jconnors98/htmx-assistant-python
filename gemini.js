// gemini.js
import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
import { searchTrustedSources } from "./searchCSE.js";

dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

export async function askGemini(userQuery) {
  const results = await searchTrustedSources(userQuery);

  const sourcesText = results.length
    ? results.map((r, i) =>
        `${i + 1}. **${r.title}**\n${r.snippet}\n[Read more](${r.link})\n`
      ).join("\n")
    : "*No matching content found in trusted sites.*";

  const prompt = `
You are a helpful assistant answering user questions using information from trusted sources first.

The user asked:
"${userQuery}"

Here are the most relevant results from trusted partner websites:

${sourcesText}

Based on this, write a helpful answer. Be warm, concise, and use markdown. If no results are relevant, say so and offer general guidance.
`;

  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });
  const chat = model.startChat({ history: [] });

  const result = await chat.sendMessage(prompt);
  const response = await result.response;
  return response.text();
}
