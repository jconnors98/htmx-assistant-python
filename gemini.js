// gemini.js
import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// Add your trusted domains here:
const trustedDomains = [
  "https://www.bccassn.com",
  "https://www.skilledtradesbc.ca",
  "https://www.talentcentral.ca",
  "https://vrca.ca/",
  "https://vicabc.ca/",
  "https://nrca.ca/",
  "https://sicabc.ca",
  "https://www.itabc.ca",
  "https://talentcentral.ca",
  "https://tradestrainingbc.ca",
  "https://www.red-seal.ca/",
  "https://www.technicalsafetybc.ca/",
"https://thetailgatetoolkit.ca/",
"https://builderscode.ca",
];

export async function askGemini(userQuery) {
  const prompt = `
You are a warm, helpful assistant supporting users on the TalentCentral platform to find construction jobs from TalentCentral.ca, training, apprenticeships, and workforce programs in British Columbia from our partners' sites and if not available, others from Canada.

### Priorities:
1. **Always search these trusted sources first**:
${trustedDomains.map((d) => "- " + d).join("\n")}
2. **If you find nothing helpful above**, then search the general web.
3. **Avoid sources like icba.ca, reddit.com, quora.com, or forums unless no other option exists.**

### Instructions:
- Provide clear, friendly answers
- Use markdown for formatting
- Include links to official programs when available

User question: "${userQuery}"
`;

  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });

  const chat = model.startChat({ history: [] });

  const result = await chat.sendMessage(prompt);
  const response = await result.response;
  return response.text();
}
