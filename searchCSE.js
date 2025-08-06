// searchCSE.js
import axios from "axios";
import dotenv from "dotenv";
dotenv.config();

const API_KEY = process.env.GOOGLE_API_KEY;  // Your Google Cloud API key
const CSE_ID = process.env.CSE_ID;           // Your Custom Search Engine ID

export async function searchTrustedSources(query) {
  const url = `https://www.googleapis.com/customsearch/v1?q=${encodeURIComponent(query)}&key=${API_KEY}&cx=${CSE_ID}`;

  const res = await axios.get(url);
  const items = res.data.items || [];

  return items.map(item => ({
    title: item.title,
    link: item.link,
    snippet: item.snippet
  }));
}
