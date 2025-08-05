// test-gemini.mjs
import { GoogleGenerativeAI } from "@google/generative-ai";

// Paste your Makersuite key directly here for this test only
const genAI = new GoogleGenerativeAI("AIzaSyBGecb9J_ZaOK80lW_xSdZkB71fIhcqQCs");

const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });
const chat = model.startChat({ history: [] });

const result = await chat.sendMessage("Say hello from Gemini.");
console.log(await result.response.text());


