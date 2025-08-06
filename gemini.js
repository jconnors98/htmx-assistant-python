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
        `${i + 1}. [${r.title}](${r.link}) – ${r.snippet}`
      ).join("\n\n")
    : "_No matching content found in trusted sites._";

  const prompt = `
You are a warm, helpful assistant supporting users on the TalentCentral platform which has job postings for construction jobs and partners the trusted sites to offer programs for workforce and workplace development, benefits, training.

### Instructions:
- Answer the user's question based **first** on the provided trusted search results below.
- Use a clear and friendly tone.
- When referring to any resource, use a **Markdown link** like [Title](https://example.com).
- If no relevant info is found, offer a helpful general answer.

### User question:
"${userQuery}"

### Trusted search results:
${sourcesText}

Now write your answer using the most relevant info above.
`;

  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });
  const chat = model.startChat({ history: [] });

  const result = await chat.sendMessage(prompt);
  const response = await result.response;
  return response.text();
}
