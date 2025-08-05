// gemini.js
import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

export async function askGemini(userQuery) {
  const prompt = `
You're a warm, helpful assistant supporting users on the TalentCentral platform.
You help with construction jobs, training, apprenticeships, and workforce programs in British Columbia.
Use a friendly, clear tone and provide links where appropriate using markdown.

User question: "${userQuery}"
`;

  const model = genAI.getGenerativeModel({ model: "gemini-pro" });

  const chat = model.startChat({ history: [] });

  const result = await chat.sendMessage(prompt);
  const response = await result.response;
  return response.text();
}
