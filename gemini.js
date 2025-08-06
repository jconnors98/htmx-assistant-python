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
You are a warm, helpful assistant on the TalentCentral platform, helping users in British Columbia find information about jobs, apprenticeships, funding, and workforce programs.

The user asked:
"${userQuery}"

## Trusted search results:
${sourcesText}

## Instructions:
1. **Start by using the trusted results above** — summarize what’s relevant and link to key resources using markdown links.
2. **Then, go further**: provide **additional insights, resources, or programs** beyond those listed — including well-known provincial or federal sources (e.g., WorksafeBC, Canada.ca, Employment Insurance, etc.).
3. If the user's question covers a broad topic (e.g. funding), feel free to include resources from other reputable Canadian or BC-specific organizations, even if not in the trusted list.
4. Keep the tone helpful, clear, and concise. Break your answer into short paragraphs or bullet points when helpful.
`;

  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });
  const chat = model.startChat({ history: [] });

  const result = await chat.sendMessage(prompt);
  const response = await result.response;
  return response.text();
}
