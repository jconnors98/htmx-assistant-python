// gemini.js
import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";
dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ✅ Your trusted partner websites
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
  "https://buildingbuilders.ca/"
  "https://https://bccassn.com/skilled-workforce/bcca-integrating-newcomers/"
  "https://bccassn.com/get-involved/employee-benefits/",
  "https://bccassn.com/past-chair-legacy-fund/",
  "https://bccassn.com/lng-canada-workforce-development-program/",
  "https://bccassn.com/connect/",
  "https://bccassn.com/get-involved/safety/"
];

export async function askGemini(userQuery) {
  const prompt = `
You are a warm, helpful assistant supporting users on the TalentCentral platform. You help them find construction jobs from TalentCentral.ca, training, apprenticeships, and workforce programs in British Columbia — starting with content from our trusted partners' websites.

### Trusted Sources (Search Priority):
Start by searching the following partner websites **for relevant information only**:
${trustedDomains.map((d) => "- " + d).join("\n")}

If you find useful content from these sources, you may:
- Mention the specific program, organization, or resource
- Include a helpful **direct link** to that page using markdown

### Important Guidelines:
- ✅ Only mention a trusted site **if it has directly useful info**
- 🚫 **Never list the full set of trusted domains** unless the user asks for it
- 🚫 Avoid using or referencing sites like icba.ca, reddit.com, quora.com, or forums unless absolutely necessary
- ✅ If no trusted content is helpful, **search the broader Canadian web**

### Response Style:
- Use a friendly, professional tone
- Use markdown formatting (e.g. [Program Name](link))
- Prefer linking to specific program or resource pages — not homepages
- Don’t explain the search process (e.g. don’t say “I searched the following sites…”)
- Keep the answer focused, useful, and clean

User question: "${userQuery}"
`;

  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });

  const chat = model.startChat({ history: [] });

  const result = await chat.sendMessage(prompt);
  const response = await result.response;
  return response.text();
}
